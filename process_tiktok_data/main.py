import functions_framework
from google.cloud import storage
from google.cloud import bigquery
import logging
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime

# Set up logging
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
        "is_verified": False
    }
    try:
        user_info = json_data["__DEFAULT_SCOPE__"]["webapp.user-detail"]["userInfo"]
        user = user_info.get("user", {})
        stats = user_info.get("stats", {})
        profile_data["username"] = user.get("uniqueId", "N/A")
        profile_data["user_id"] = user.get("id", "N/A")
        profile_data["actual_name"] = user.get("nickname", "Unknown")
        profile_data["following_count"] = stats.get("followingCount", 0)
        profile_data["follower_count"] = stats.get("followerCount", 0)
        profile_data["total_like_count"] = stats.get("heartCount", 0)
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
            logger.info("No posts found in JSON under 'posts', trying alternative structure.")
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
                create_time = post.get("createTime", "N/A")
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
                    "share_count": share_count
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
        "is_verified": False
    }
    try:
        # Username
        username_tag = soup.find("h2", {"data-e2e": "user-subtitle"})
        profile_data["username"] = username_tag.text.strip() if username_tag else "N/A"
        
        # Actual name
        name_tag = soup.find("h1", {"data-e2e": "user-title"})
        profile_data["actual_name"] = name_tag.text.strip() if name_tag else "Unknown"
        
        # Stats
        stats = soup.find_all("strong", {"data-e2e": "user-stats"})
        profile_data["following_count"] = int(re.sub(r'[^\d]', '', stats[0].text)) if stats and len(stats) > 0 else 0
        profile_data["follower_count"] = int(re.sub(r'[^\d]', '', stats[1].text)) if stats and len(stats) > 1 else 0
        profile_data["total_like_count"] = int(re.sub(r'[^\d]', '', stats[2].text)) if stats and len(stats) > 2 else 0
        
        # Caption/Bio
        bio_tag = soup.find("h2", {"data-e2e": "user-bio"})
        profile_data["bio"] = bio_tag.text.strip() if bio_tag else "N/A"
        profile_data["caption"] = profile_data["bio"]
        
        # Bio link
        link_tag = soup.find("a", {"data-e2e": "user-link"})
        profile_data["bio_link"] = link_tag["href"] if link_tag else "No link"
        
        # Profile picture
        pic_tag = soup.find("img", {"data-e2e": "user-avatar"})
        profile_data["profile_pic_url"] = pic_tag["src"] if pic_tag else "N/A"
        
        # Verified status
        verified_tag = soup.find("svg", {"data-e2e": "verify-badge"})
        profile_data["is_verified"] = bool(verified_tag)
        
        logger.info("Extracted profile data from HTML successfully.")
    except Exception as e:
        logger.error(f"Error extracting profile data from HTML: {e}")
    return profile_data

# --- Extract Video Data from HTML (Fallback) ---
def extract_video_data_from_html(soup, username):
    videos_data = []
    try:
        video_containers = soup.find_all("div", {"data-e2e": "user-post-item"})
        for container in video_containers:
            try:
                a_tag = container.find("a")
                url = a_tag["href"] if a_tag else "N/A"
                views_tag = container.find("strong", {"data-e2e": "video-views"})
                views = int(re.sub(r'[^\d]', '', views_tag.text)) if views_tag else 0
                img_tag = container.find("img")
                thumbnail = img_tag["src"] if img_tag else "N/A"
                # Additional video details (if available in HTML)
                description = container.find("div", {"data-e2e": "video-desc"})
                description = description.text.strip() if description else "N/A"
                videos_data.append({
                    "url": url,
                    "views": views,
                    "thumbnail": thumbnail,
                    "description": description,
                    "create_time": "N/A",  # Not typically in HTML
                    "like_count": 0,       # Not typically in HTML
                    "comment_count": 0,    # Not typically in HTML
                    "share_count": 0       # Not typically in HTML
                })
            except Exception as e:
                logger.error(f"Error parsing video container: {e}")
                continue
        logger.info(f"Extracted {len(videos_data)} videos from HTML.")
    except Exception as e:
        logger.error(f"Error extracting video data from HTML: {e}")
    return videos_data

@functions_framework.cloud_event
def process_tiktok_data(cloud_event):
    try:
        # Get the file details from the Cloud Storage event
        event_data = cloud_event.data
        bucket_name = event_data["bucket"]
        file_name = event_data["name"]
        logger.info(f"Processing file: gs://{bucket_name}/{file_name}")

        # Download the HTML file from GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        html_content = blob.download_as_text()
        logger.info("Downloaded HTML file.")

        # Parse the HTML with BeautifulSoup
        soup = BeautifulSoup(html_content, "html.parser")

        # Extract data from JSON (preferred method)
        script_tag = soup.find("script", id="__UNIVERSAL_DATA_FOR_REHYDRATION__")
        profile_data = None
        videos_data = []
        username = file_name.split("/")[-2]  # e.g., jordanesaget

        if script_tag and script_tag.string:
            json_data = json.loads(script_tag.string.strip())
            if "__DEFAULT_SCOPE__" in json_data and "webapp.user-detail" in json_data["__DEFAULT_SCOPE__"]:
                logger.info("JSON data found, extracting data...")
                profile_data = extract_profile_data_from_json(json_data)
                videos_data = extract_video_data_from_json(json_data, profile_data["username"])
        
        # Fallback to HTML parsing if JSON data is incomplete
        if not profile_data or profile_data["username"] == "N/A":
            logger.info("Falling back to HTML parsing for profile data.")
            profile_data = extract_profile_data_from_html(soup)
        
        if not videos_data:
            logger.info("Falling back to HTML parsing for video data.")
            videos_data = extract_video_data_from_html(soup, username)

        # Add scrape timestamp
        profile_data["scrape_timestamp"] = event_data["timeCreated"]

        # Log extracted data
        logger.info(f"Extracted profile data: {profile_data}")
        logger.info(f"Extracted {len(videos_data)} videos: {videos_data}")

        # Save profile data to GCS
        processed_bucket_name = "tiktok-processed-data"
        profile_blob_path = f"profiles/{username}/{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.json"
        profile_blob = storage_client.bucket(processed_bucket_name).blob(profile_blob_path)
        profile_blob.upload_from_string(json.dumps(profile_data), content_type="application/json")
        logger.info(f"Saved processed profile data to GCS: {profile_blob_path}")

        # Save video data to GCS
        if videos_data:
            video_blob_path = f"videos/{username}/{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.json"
            video_blob = storage_client.bucket(processed_bucket_name).blob(video_blob_path)
            video_blob.upload_from_string(json.dumps(videos_data), content_type="application/json")
            logger.info(f"Saved processed video data to GCS: {video_blob_path}")

        # Load profile data into BigQuery
        bq_client = bigquery.Client()
        profile_table_id = "training-triggering-pipeline.tiktok_dataset.profiles"
        profile_job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=[
                bigquery.SchemaField("username", "STRING"),
                bigquery.SchemaField("user_id", "STRING"),
                bigquery.SchemaField("actual_name", "STRING"),
                bigquery.SchemaField("following_count", "INTEGER"),
                bigquery.SchemaField("follower_count", "INTEGER"),
                bigquery.SchemaField("total_like_count", "INTEGER"),
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
        logger.info(f"Loaded profile data into BigQuery table {profile_table_id}")

        # Load video data into BigQuery
        if videos_data:
            video_blob_path = f"videos/{username}/{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}.json"
            video_blob = storage_client.bucket(processed_bucket_name).blob(video_blob_path)
            # Write each video as a separate line (newline-delimited JSON)
            video_content = "\n".join(json.dumps(video) for video in videos_data)
            video_blob.upload_from_string(video_content, content_type="application/json")
            logger.info(f"Saved processed video data to GCS: {video_blob_path}")

            # Load video data into BigQuery
            video_table_id = "training-triggering-pipeline.tiktok_dataset.videos"
            video_job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=[
                    bigquery.SchemaField("url", "STRING"),
                    bigquery.SchemaField("views", "INTEGER"),
                    bigquery.SchemaField("thumbnail", "STRING"),
                    bigquery.SchemaField("description", "STRING"),
                    bigquery.SchemaField("create_time", "STRING"),
                    bigquery.SchemaField("like_count", "INTEGER"),
                    bigquery.SchemaField("comment_count", "INTEGER"),
                    bigquery.SchemaField("share_count", "INTEGER")
                ]
            )
            video_uri = f"gs://{processed_bucket_name}/{video_blob_path}"
            video_load_job = bq_client.load_table_from_uri(video_uri, video_table_id, job_config=video_job_config)
            video_load_job.result()  # Wait for the job to complete
            if video_load_job.errors:
                logger.error(f"Errors loading video data into BigQuery: {video_load_job.errors}")
                raise Exception(f"Failed to load video data: {video_load_job.errors}")
            logger.info(f"Loaded video data into BigQuery table {video_table_id}")

    except Exception as e:
        logger.error(f"Error in process_tiktok_data: {str(e)}")
        raise