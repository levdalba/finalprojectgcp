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
        "total_like_count": 0,
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
        heart_count = stats.get("heartCount", 0)
        logger.info(f"Raw heartCount for {profile_data['username']}: {heart_count} (type: {type(heart_count)})")
        try:
            profile_data["total_like_count"] = int(float(heart_count)) if heart_count else 0
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing heartCount for {profile_data['username']}: {heart_count}, error: {e}")
            profile_data["total_like_count"] = 0
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
def extract_video_data_from_json(json_data, username):
    videos_data = []
    try:
        posts = json_data["__DEFAULT_SCOPE__"]["webapp.user-detail"].get("posts", [])
        if not posts:
            logger.info("No posts found in 'posts', trying alternative structure.")
            posts = (
                json_data["__DEFAULT_SCOPE__"]["webapp.user-detail"]["userInfo"]
                .get("stats", {})
                .get("videoList", [])
            )
        for post in posts:
            try:
                video_id = post["id"]
                url = f"https://www.tiktok.com/@{username}/video/{video_id}"
                views = post["stats"]["playCount"]
                thumbnail = post["video"]["cover"]
                description = post.get("desc", "N/A")
                create_time = post.get("createTime", "N/A")  # Unix timestamp or N/A
                like_count = post["stats"].get("diggCount", 0)
                comment_count = post["stats"].get("commentCount", 0)
                share_count = post["stats"].get("shareCount", 0)
                videos_data.append({
                    "url": url,
                    "views": views,
                    "thumbnail": thumbnail,
                    "description": description,
                    "create_time": create_time,
                    "like_count": like_count,
                    "comment_count": comment_count,
                    "share_count": share_count,
                    "scrape_timestamp": None  # Will be set later
                })
            except KeyError as e:
                logger.error(f"KeyError extracting video data for a post: {e}")
                continue
        logger.info(f"Extracted {len(videos_data)} videos from JSON.")
    except KeyError as e:
        logger.error(f"KeyError accessing posts in JSON: {e}")
    return videos_data

# --- Extract Profile Data from HTML (Fallback) ---
def extract_profile_data_from_html(soup):
    profile_data = {
        "username": "N/A",
        "user_id": "N/A",
        "actual_name": "Unknown",
        "following_count": 0,
        "follower_count": 0,
        "total_like_count": 0,
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
        
        # Handle total_like_count with suffixes (e.g., 11B, 1.5M, 500K)
        like_text = re.sub(r'[^\dBMK.]', '', stats[2].text) if stats and len(stats) > 2 else "0"
        if 'B' in like_text:
            profile_data["total_like_count"] = int(float(like_text.replace('B', '')) * 1_000_000_000)
        elif 'M' in like_text:
            profile_data["total_like_count"] = int(float(like_text.replace('M', '')) * 1_000_000)
        elif 'K' in like_text:
            profile_data["total_like_count"] = int(float(like_text.replace('K', '')) * 1_000)
        else:
            profile_data["total_like_count"] = int(like_text)

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
def extract_video_data_from_html(soup, username):
    videos = []
    try:
        video_elements = soup.select('div[data-e2e="user-post-item"]')
        logger.info(f"Found {len(video_elements)} video elements in HTML for {username}")
        for video_elem in video_elements:
            # Extract URL
            url_elem = video_elem.find('a', href=True)
            url = url_elem['href'] if url_elem else f"https://www.tiktok.com/@{username}/video/unknown"
            if not url.startswith("https://www.tiktok.com"):
                url = f"https://www.tiktok.com{url}"

            # Extract Views
            view_count_elem = video_elem.find('strong', {'data-e2e': 'video-views'})
            views = 0
            if view_count_elem:
                view_text = view_count_elem.text
                if 'M' in view_text:
                    views = int(float(view_text.replace('M', '')) * 1000000)
                elif 'K' in view_text:
                    views = int(float(view_text.replace('K', '')) * 1000)
                else:
                    views = int(view_text)

            # Extract Thumbnail
            thumbnail_elem = video_elem.find('img')
            thumbnail = thumbnail_elem['src'] if thumbnail_elem else ""

            # Extract Description
            description_elem = video_elem.find('h3', {'data-e2e': 'video-desc'}) or video_elem.find('div', {'class': 'tiktok-1itcwxg-DivDescription'})
            description = description_elem.text.strip() if description_elem else "N/A"
            logger.info(f"Description for {url}: {description}")

            # Attempt to Extract Likes, Comments, Shares (these might not be available in HTML)
            stats_elem = video_elem.find('div', {'class': 'tiktok-1g0p768-DivStats'})
            like_count = 0
            comment_count = 0
            share_count = 0
            if stats_elem:
                like_elem = stats_elem.find('strong', {'data-e2e': 'like-count'})
                like_count = int(like_elem.text.replace('M', '000000').replace('K', '000')) if like_elem else 0
                comment_elem = stats_elem.find('strong', {'data-e2e': 'comment-count'})
                comment_count = int(comment_elem.text.replace('M', '000000').replace('K', '000')) if comment_elem else 0
                share_elem = stats_elem.find('strong', {'data-e2e': 'share-count'})
                share_count = int(share_elem.text.replace('M', '000000').replace('K', '000')) if share_elem else 0

            videos.append({
                "url": url,
                "views": views,
                "thumbnail": thumbnail,
                "description": description,
                "create_time": "",  # Still not available in HTML
                "like_count": like_count,
                "comment_count": comment_count,
                "share_count": share_count
            })
    except Exception as e:
        logger.error(f"Error extracting videos from HTML for {username}: {str(e)}")
    return videos
@functions_framework.cloud_event
def process_tiktok_data(cloud_event):
    try:
        event_data = cloud_event.data
        bucket_name = event_data["bucket"]
        file_name = event_data["name"]
        logger.info(f"Processing file: gs://{bucket_name}/{file_name}")

        # Extract username from file path
        username = file_name.split("/")[-2]  # e.g., bellapoarch

        # Delete existing data for this username
        bq_client = bigquery.Client()
        profile_table_id = "training-triggering-pipeline.tiktok_dataset.profiles"
        video_table_id = "training-triggering-pipeline.tiktok_dataset.videos"
        delete_profile_query = f"""
        DELETE FROM `{profile_table_id}`
        WHERE username = '{username}'
        """
        delete_video_query = f"""
        DELETE FROM `{video_table_id}`
        WHERE url LIKE '%@{username}%'
        """
        bq_client.query(delete_profile_query).result()
        bq_client.query(delete_video_query).result()
        logger.info(f"Deleted existing data for username: {username}")

        # Proceed with processing
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        html_content = blob.download_as_text()
        logger.info("Downloaded HTML file.")

        soup = BeautifulSoup(html_content, "html.parser")
        script_tag = soup.find("script", id="__UNIVERSAL_DATA_FOR_REHYDRATION__")
        profile_data = None
        videos_data = []
        scrape_timestamp = event_data["timeCreated"]

        if script_tag and script_tag.string:
            json_data = json.loads(script_tag.string.strip())
            if "__DEFAULT_SCOPE__" in json_data and "webapp.user-detail" in json_data["__DEFAULT_SCOPE__"]:
                logger.info("JSON data found, extracting data...")
                profile_data = extract_profile_data_from_json(json_data)
                videos_data = extract_video_data_from_json(json_data, profile_data["username"])

        if not profile_data or profile_data["username"] == "N/A":
            logger.info("Falling back to HTML parsing for profile data.")
            profile_data = extract_profile_data_from_html(soup)

        if not videos_data:
            logger.info("Falling back to HTML parsing for video data.")
            videos_data = extract_video_data_from_html(soup, username)

        # Add scrape timestamp to all records
        profile_data["scrape_timestamp"] = scrape_timestamp
        for video in videos_data:
            video["scrape_timestamp"] = scrape_timestamp

        logger.info(f"Extracted profile data: {profile_data}")
        logger.info(f"Extracted {len(videos_data)} videos.")

        # Save to GCS
        processed_bucket_name = "tiktok-processed-data"
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        profile_blob_path = f"profiles/{username}/{timestamp}.json"
        profile_blob = storage_client.bucket(processed_bucket_name).blob(profile_blob_path)
        profile_blob.upload_from_string(json.dumps(profile_data), content_type="application/json")
        logger.info(f"Saved profile data to GCS: {profile_blob_path}")

        if videos_data:
            video_blob_path = f"videos/{username}/{timestamp}.json"
            video_blob = storage_client.bucket(processed_bucket_name).blob(video_blob_path)
            video_content = "\n".join(json.dumps(video) for video in videos_data)  # NDJSON
            video_blob.upload_from_string(video_content, content_type="application/json")
            logger.info(f"Saved video data to GCS: {video_blob_path}")

        # Load to BigQuery
        profile_job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("username", "STRING", "REQUIRED"),
                bigquery.SchemaField("user_id", "STRING", "REQUIRED"),
                bigquery.SchemaField("actual_name", "STRING"),
                bigquery.SchemaField("following_count", "INTEGER"),
                bigquery.SchemaField("follower_count", "INTEGER"),
                bigquery.SchemaField("total_like_count", "STRING"),  # Changed to STRING
                bigquery.SchemaField("caption", "STRING"),
                bigquery.SchemaField("bio_link", "STRING"),
                bigquery.SchemaField("bio", "STRING"),
                bigquery.SchemaField("profile_pic_url", "STRING"),
                bigquery.SchemaField("is_verified", "BOOLEAN"),
                bigquery.SchemaField("scrape_timestamp", "TIMESTAMP")
            ]
        )
        profile_uri = f"gs://{processed_bucket_name}/{profile_blob_path}"
        profile_load_job = bq_client.load_table_from_uri(profile_uri, profile_table_id, job_config=profile_job_config)
        profile_load_job.result()
        logger.info(f"Loaded profile data into BigQuery: {profile_table_id}")

        if videos_data:
            video_job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[
                    bigquery.SchemaField("url", "STRING", "REQUIRED"),
                    bigquery.SchemaField("views", "INTEGER"),
                    bigquery.SchemaField("thumbnail", "STRING"),
                    bigquery.SchemaField("description", "STRING"),
                    bigquery.SchemaField("create_time", "STRING"),
                    bigquery.SchemaField("like_count", "INTEGER"),
                    bigquery.SchemaField("comment_count", "INTEGER"),
                    bigquery.SchemaField("share_count", "INTEGER"),
                    bigquery.SchemaField("scrape_timestamp", "TIMESTAMP")
                ]
            )
            video_uri = f"gs://{processed_bucket_name}/{video_blob_path}"
            video_load_job = bq_client.load_table_from_uri(video_uri, video_table_id, job_config=video_job_config)
            video_load_job.result()
            if video_load_job.errors:
                logger.error(f"Errors loading video data: {video_load_job.errors}")
                raise Exception(f"Failed to load video data: {video_load_job.errors}")
            logger.info(f"Loaded video data into BigQuery: {video_table_id}")

    except Exception as e:
        logger.error(f"Error in process_tiktok_data: {str(e)}")
        raise
    try:
        event_data = cloud_event.data
        bucket_name = event_data["bucket"]
        file_name = event_data["name"]
        logger.info(f"Processing file: gs://{bucket_name}/{file_name}")

        # Extract username from file path
        username = file_name.split("/")[-2]  # e.g., jasonmoments

        # Delete existing data for this username
        bq_client = bigquery.Client()
        profile_table_id = "training-triggering-pipeline.tiktok_dataset.profiles"
        video_table_id = "training-triggering-pipeline.tiktok_dataset.videos"
        delete_profile_query = f"""
        DELETE FROM `{profile_table_id}`
        WHERE username = '{username}'
        """
        delete_video_query = f"""
        DELETE FROM `{video_table_id}`
        WHERE url LIKE '%@{username}%'
        """
        bq_client.query(delete_profile_query).result()
        bq_client.query(delete_video_query).result()
        logger.info(f"Deleted existing data for username: {username}")

        # Proceed with processing
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        html_content = blob.download_as_text()
        logger.info("Downloaded HTML file.")

        soup = BeautifulSoup(html_content, "html.parser")
        script_tag = soup.find("script", id="__UNIVERSAL_DATA_FOR_REHYDRATION__")
        profile_data = None
        videos_data = []
        scrape_timestamp = event_data["timeCreated"]

        if script_tag and script_tag.string:
            json_data = json.loads(script_tag.string.strip())
            if "__DEFAULT_SCOPE__" in json_data and "webapp.user-detail" in json_data["__DEFAULT_SCOPE__"]:
                logger.info("JSON data found, extracting data...")
                profile_data = extract_profile_data_from_json(json_data)
                videos_data = extract_video_data_from_json(json_data, profile_data["username"])

        if not profile_data or profile_data["username"] == "N/A":
            logger.info("Falling back to HTML parsing for profile data.")
            profile_data = extract_profile_data_from_html(soup)

        if not videos_data:
            logger.info("Falling back to HTML parsing for video data.")
            videos_data = extract_video_data_from_html(soup, username)

        # Add scrape timestamp to all records
        profile_data["scrape_timestamp"] = scrape_timestamp
        for video in videos_data:
            video["scrape_timestamp"] = scrape_timestamp

        logger.info(f"Extracted profile data: {profile_data}")
        logger.info(f"Extracted {len(videos_data)} videos.")

        # Save to GCS
        processed_bucket_name = "tiktok-processed-data"
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        profile_blob_path = f"profiles/{username}/{timestamp}.json"
        profile_blob = storage_client.bucket(processed_bucket_name).blob(profile_blob_path)
        profile_blob.upload_from_string(json.dumps(profile_data), content_type="application/json")
        logger.info(f"Saved profile data to GCS: {profile_blob_path}")

        if videos_data:
            video_blob_path = f"videos/{username}/{timestamp}.json"
            video_blob = storage_client.bucket(processed_bucket_name).blob(video_blob_path)
            video_content = "\n".join(json.dumps(video) for video in videos_data)  # NDJSON
            video_blob.upload_from_string(video_content, content_type="application/json")
            logger.info(f"Saved video data to GCS: {video_blob_path}")

        # Load to BigQuery
        profile_job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("username", "STRING", "REQUIRED"),
                bigquery.SchemaField("user_id", "STRING", "REQUIRED"),
                bigquery.SchemaField("actual_name", "STRING"),
                bigquery.SchemaField("following_count", "INT64"),
                bigquery.SchemaField("follower_count", "INT64"),
                bigquery.SchemaField("total_like_count", "STRING"),
                bigquery.SchemaField("caption", "STRING"),
                bigquery.SchemaField("bio_link", "STRING"),
                bigquery.SchemaField("bio", "STRING"),
                bigquery.SchemaField("profile_pic_url", "STRING"),
                bigquery.SchemaField("is_verified", "BOOLEAN"),
                bigquery.SchemaField("scrape_timestamp", "TIMESTAMP")
            ]
        )
        profile_uri = f"gs://{processed_bucket_name}/{profile_blob_path}"
        profile_load_job = bq_client.load_table_from_uri(profile_uri, profile_table_id, job_config=profile_job_config)
        profile_load_job.result()
        logger.info(f"Loaded profile data into BigQuery: {profile_table_id}")

        if videos_data:
            video_job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[
                    bigquery.SchemaField("url", "STRING", "REQUIRED"),
                    bigquery.SchemaField("views", "INT64"),
                    bigquery.SchemaField("thumbnail", "STRING"),
                    bigquery.SchemaField("description", "STRING"),
                    bigquery.SchemaField("create_time", "STRING"),
                    bigquery.SchemaField("like_count", "INT64"),
                    bigquery.SchemaField("comment_count", "INT64"),
                    bigquery.SchemaField("share_count", "INT64"),
                    bigquery.SchemaField("scrape_timestamp", "TIMESTAMP")
                ]
            )
            video_uri = f"gs://{processed_bucket_name}/{video_blob_path}"
            video_load_job = bq_client.load_table_from_uri(video_uri, video_table_id, job_config=video_job_config)
            video_load_job.result()
            if video_load_job.errors:
                logger.error(f"Errors loading video data: {video_load_job.errors}")
                raise Exception(f"Failed to load video data: {video_load_job.errors}")
            logger.info(f"Loaded video data into BigQuery: {video_table_id}")

    except Exception as e:
        logger.error(f"Error in process_tiktok_data: {str(e)}")
        raise