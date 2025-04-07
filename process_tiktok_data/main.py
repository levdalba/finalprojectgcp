import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import logging
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Extract Profile Data from JSON ---
def extract_profile_data_from_json(json_data):
    profile_data = {
        "username": "N/A",
        "user_id": "N/A",
        "actual_name": "Unknown",
        "following_count": 0,
        "follower_count": 0,
        "total_like_count": "0",  # Store as string
        "caption": "N/A",
        "bio_link": "No link",
        "bio": "N/A",
        "profile_pic_url": "N/A",
        "is_verified": False,
        "scrape_timestamp": None
    }
    try:
        user_info = json_data["__DEFAULT_SCOPE__"]["webapp.user-detail"]["userInfo"]
        user = user_info.get("user", {})
        stats = user_info.get("stats", {})
        profile_data["username"] = user.get("uniqueId", "N/A")
        profile_data["user_id"] = user.get("id", "N/A")
        profile_data["actual_name"] = user.get("nickname", "Unknown")
        profile_data["following_count"] = int(stats.get("followingCount", 0))
        profile_data["follower_count"] = int(stats.get("followerCount", 0))
        
        heart_count = stats.get("heartCount", "0")
        logger.info(f"Raw heartCount for {profile_data['username']}: {heart_count} (type: {type(heart_count)})")
        try:
            if isinstance(heart_count, str):
                heart_count = heart_count.upper().replace(',', '')
                if 'B' in heart_count:
                    total_likes = int(float(heart_count.replace('B', '')) * 1_000_000_000)
                elif 'M' in heart_count:
                    total_likes = int(float(heart_count.replace('M', '')) * 1_000_000)
                elif 'K' in heart_count:
                    total_likes = int(float(heart_count.replace('K', '')) * 1_000)
                else:
                    total_likes = int(float(heart_count))
            else:
                total_likes = int(float(heart_count))
            if total_likes < 0:
                logger.warning(f"Negative total_likes: {total_likes}, setting to 0")
                total_likes = 0
            profile_data["total_like_count"] = str(total_likes)
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing heartCount for {profile_data['username']}: {heart_count}, error: {e}")
            profile_data["total_like_count"] = "0"
        
        profile_data["caption"] = user.get("signature", "N/A")
        profile_data["bio_link"] = user.get("bioLink", {}).get("link", "No link")
        profile_data["bio"] = user.get("bio", "N/A")
        profile_data["profile_pic_url"] = user.get("avatarLarger", "N/A")
        profile_data["is_verified"] = user.get("verified", False)
        logger.info("Extracted profile data from JSON successfully.")
    except KeyError as e:
        logger.error(f"KeyError extracting profile data: {e}")
    return profile_data

# --- Extract Video Data from JSON ---
def extract_video_data_from_json(soup):
    """Extract video data from the JSON script tag."""
    try:
        script_tag = soup.find("script", id="__UNIVERSAL_DATA_FOR_REHYDRATION__")
        if not script_tag:
            logger.info("No __UNIVERSAL_DATA_FOR_REHYDRATION__ script tag found, falling back to HTML parsing")
            return []

        json_data = json.loads(script_tag.string)
        videos = []
        try:
            posts = json_data["__DEFAULT_SCOPE__"]["webapp.user-detail"]["userInfo"]["stats"]["videoList"]
        except KeyError as e:
            logger.error(f"KeyError accessing posts in JSON: {e}")
            return []

        for post in posts:
            video = {
                "url": post.get("videoUrl", ""),
                "views": post.get("playCount", 0),
                "likes": post.get("diggCount", 0),
                "comments": post.get("commentCount", 0),
                "shares": post.get("shareCount", 0),
                "thumbnail": post.get("thumbnailUrl", ""),
                "description": post.get("desc", ""),
                "create_time": post.get("createTime", "")
            }
            videos.append(video)
        return videos
    except Exception as e:
        logger.error(f"Error extracting video data from JSON: {e}")
        return []


# --- Extract Profile Data from HTML (Fallback) ---
def extract_profile_data_from_html(soup):
    profile_data = {
        "username": "N/A",
        "user_id": "N/A",
        "actual_name": "Unknown",
        "following_count": 0,
        "follower_count": 0,
        "total_like_count": "0",
        "caption": "N/A",
        "bio_link": "No link",
        "bio": "N/A",
        "profile_pic_url": "N/A",
        "is_verified": False,
        "scrape_timestamp": None
    }
    try:
        username_tag = soup.find("h2", {"data-e2e": "user-subtitle"})
        profile_data["username"] = username_tag.text.strip() if username_tag else "N/A"
        name_tag = soup.find("h1", {"data-e2e": "user-title"})
        profile_data["actual_name"] = name_tag.text.strip() if name_tag else "Unknown"
        stats = soup.find_all("strong", {"data-e2e": "user-stats"})
        profile_data["following_count"] = int(re.sub(r'[^\d]', '', stats[0].text)) if stats and len(stats) > 0 else 0
        profile_data["follower_count"] = int(re.sub(r'[^\d]', '', stats[1].text)) if stats and len(stats) > 1 else 0
        
        like_text = re.sub(r'[^\dBMK.]', '', stats[2].text) if stats and len(stats) > 2 else "0"
        logger.info(f"Raw like_text for {profile_data['username']}: {like_text}")
        try:
            like_text = like_text.upper().replace(',', '')
            if 'B' in like_text:
                total_likes = int(float(like_text.replace('B', '')) * 1_000_000_000)
            elif 'M' in like_text:
                total_likes = int(float(like_text.replace('M', '')) * 1_000_000)
            elif 'K' in like_text:
                total_likes = int(float(like_text.replace('K', '')) * 1_000)
            else:
                total_likes = int(float(like_text))
            if total_likes < 0:
                logger.warning(f"Negative total_likes: {total_likes}, setting to 0")
                total_likes = 0
            profile_data["total_like_count"] = str(total_likes)
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing like_text: {like_text}, error: {e}")
            profile_data["total_like_count"] = "0"

        bio_tag = soup.find("h2", {"data-e2e": "user-bio"})
        profile_data["bio"] = bio_tag.text.strip() if bio_tag else "N/A"
        profile_data["caption"] = profile_data["bio"]
        link_tag = soup.find("a", {"data-e2e": "user-link"})
        profile_data["bio_link"] = link_tag["href"] if link_tag else "No link"
        pic_tag = soup.find("img", {"data-e2e": "user-avatar"})
        profile_data["profile_pic_url"] = pic_tag["src"] if pic_tag else "N/A"
        verified_tag = soup.find("svg", {"data-e2e": "verify-badge"})
        profile_data["is_verified"] = bool(verified_tag)
        logger.info("Extracted profile data from HTML successfully.")
    except Exception as e:
        logger.error(f"Error extracting profile data from HTML: {e}")
    return profile_data

# --- Extract Video Data from HTML (Fallback) ---
def extract_video_data_from_html(soup):
    """Extract video data directly from HTML by parsing <a> tags."""
    videos = []
    # Find all <a> tags that link to TikTok videos
    video_links = soup.find_all("a", href=re.compile(r"https://www.tiktok.com/@[^/]+/video/\d+"))
    
    for link in video_links:
        video_url = link["href"]
        # Find the parent container to extract additional metadata
        container = link.find_parent("div", class_=re.compile("tiktok-.*-DivItemContainer"))
        if not container:
            continue

        # Extract metadata if available
        description = container.find("div", class_=re.compile("tiktok-.*-DivVideoDesc"))
        description = description.text.strip() if description else ""

        # Try to find stats (views, likes, etc.) - these might be in child elements
        stats = container.find("div", class_=re.compile("tiktok-.*-DivStats"))
        views = 0
        likes = 0
        comments = 0
        shares = 0
        if stats:
            # These are hypothetical class names; adjust based on the actual HTML
            view_count = stats.find("span", class_=re.compile("tiktok-.*-SpanViewCount"))
            like_count = stats.find("span", class_=re.compile("tiktok-.*-SpanLikeCount"))
            comment_count = stats.find("span", class_=re.compile("tiktok-.*-SpanCommentCount"))
            share_count = stats.find("span", class_=re.compile("tiktok-.*-SpanShareCount"))
            
            views = int(view_count.text) if view_count and view_count.text.isdigit() else 0
            likes = int(like_count.text) if like_count and like_count.text.isdigit() else 0
            comments = int(comment_count.text) if comment_count and comment_count.text.isdigit() else 0
            shares = int(share_count.text) if share_count and share_count.text.isdigit() else 0

        # Extract thumbnail if available
        thumbnail = container.find("img", class_=re.compile("tiktok-.*-ImgPoster"))
        thumbnail_url = thumbnail["src"] if thumbnail and "src" in thumbnail.attrs else ""

        video = {
            "url": video_url,
            "views": views,
            "likes": likes,
            "comments": comments,
            "shares": shares,
            "thumbnail": thumbnail_url,
            "description": description,
            "create_time": ""  # We may not have this in HTML; can be enriched later
        }
        videos.append(video)
    
    return videos

@functions_framework.cloud_event
def process_tiktok_data(cloud_event):
    """Cloud Function to process TikTok HTML files and load data into BigQuery."""
    try:
        # Extract bucket and file info from the event
        data = cloud_event.data
        bucket_name = data["bucket"]
        file_name = data["name"]
        logger.info(f"Processing file: gs://{bucket_name}/{file_name}")

        # Download the HTML file from GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        html_content = blob.download_as_text()
        logger.info(f"Downloaded HTML content, length: {len(html_content)} characters")

        # Parse the HTML
        soup = BeautifulSoup(html_content, "html.parser")

        # Extract video data
        videos = extract_video_data_from_json(soup)
        if not videos:
            videos = extract_video_data_from_html(soup)

        # Log the number of videos extracted
        logger.info(f"Extracted {len(videos)} videos")

        # Load videos into BigQuery
        if videos:
            bigquery_client = bigquery.Client()
            table_id = "training-triggering-pipeline.tiktok_dataset.videos"
            table = bigquery_client.get_table(table_id)

            # Prepare video data for BigQuery (ensure create_time is STRING)
            for video in videos:
                if video["create_time"] and isinstance(video["create_time"], (int, float)):
                    video["create_time"] = datetime.datetime.fromtimestamp(video["create_time"]).strftime('%Y-%m-%d %H:%M:%S')
                elif not video["create_time"]:
                    video["create_time"] = ""

            # Load videos into BigQuery
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema=[
                    bigquery.SchemaField("url", "STRING"),
                    bigquery.SchemaField("views", "INTEGER"),
                    bigquery.SchemaField("likes", "INTEGER"),
                    bigquery.SchemaField("comments", "INTEGER"),
                    bigquery.SchemaField("shares", "INTEGER"),
                    bigquery.SchemaField("thumbnail", "STRING"),
                    bigquery.SchemaField("description", "STRING"),
                    bigquery.SchemaField("create_time", "STRING"),
                ]
            )
            video_load_job = bigquery_client.load_table_from_json(videos, table_id, job_config=job_config)
            video_load_job.result()
            logger.info(f"Loaded video data into BigQuery: {table_id}")
        else:
            logger.info("No video data to load into BigQuery.")

        # Move the processed file to tiktok-processed-data bucket
        dest_bucket_name = "tiktok-processed-data"
        dest_bucket = storage_client.bucket(dest_bucket_name)
        dest_blob = dest_bucket.blob(file_name)
        dest_blob.upload_from_string(html_content, content_type="text/html")
        logger.info(f"Copied file to gs://{dest_bucket_name}/{file_name}")

        # Delete the file from the raw bucket
        blob.delete()
        logger.info(f"Deleted file from gs://{bucket_name}/{file_name}")

        return f"Successfully processed gs://{bucket_name}/{file_name}"

    except Exception as e:
        logger.error(f"Error in process_tiktok_data: {e}")
        raise
