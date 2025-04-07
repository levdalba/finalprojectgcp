from google.cloud import pubsub_v1
from google.cloud import bigquery
import logging
import time
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = 'training-triggering-pipeline'
DATASET_ID = 'tiktok_dataset'
PUBSUB_TOPIC = 'projects/training-triggering-pipeline/topics/scrape-tiktok-topic'
MAX_LIKES = 1_000_000_000  # 1 billion
TARGET_PROFILE_COUNT = 15

# Predefined list of TikTok usernames (you can expand this list)
TIKTOK_USERNAMES = [
    "toscaa.fgl", "charlidamelio", "addisonre", "bellapoarch", "zachking",
    "khaby.lame", "willsmith", "jasonderulo", "spencerx", "lorengray",
    # "dixiedamelio", "michael.le", "justmaiko", "avani", "noahbeck",
    # "brentrivera", "therock", "kyliejenner", "billieeilish", "selenagomez",
    # "arianagrande", "kimkardashian", "davidbeckham", "neymarjr", "leomessi",
    # "cristiano", "shakira", "dualipa", "edsheeran", "justinbieber",
    # "taylorswift", "rihanna", "drake", "nickiminaj", "beyonce"
]

def publish_scrape_request(username):
    """Publish a scrape request to the Pub/Sub topic."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = PUBSUB_TOPIC
    url = f"https://www.tiktok.com/@{username}"
    message = url.encode("utf-8")
    logger.info(f"Publishing scrape request for {url}")
    publisher.publish(topic_path, message)
    logger.info(f"Published scrape request for {username}")

def get_total_likes(username):
    """Get the total likes for a username from the profile_video_summary view."""
    bq_client = bigquery.Client(project=PROJECT_ID)
    query = f"""
    SELECT total_like_count
    FROM `{PROJECT_ID}.{DATASET_ID}.profile_video_summary`
    WHERE username = '{username}'
    """
    logger.info(f"Querying total likes for {username}")
    result = bq_client.query(query).result()
    rows = list(result)
    if not rows:
        logger.warning(f"No data found in profile_video_summary for {username}")
        return None
    total_likes_str = rows[0]["total_like_count"]
    try:
        total_likes = int(total_likes_str)
        if total_likes < 0:
            logger.warning(f"Negative total_likes for {username}: {total_likes}, setting to 0")
            total_likes = 0
    except ValueError as e:
        logger.error(f"Error parsing total_likes for {username}: {total_likes_str}, error: {e}")
        total_likes = 0
    logger.info(f"Total likes for {username}: {total_likes}")
    return total_likes

def scrape_random_profiles():
    """Scrape 15 random TikTok profiles with total likes less than 1 billion."""
    selected_profiles = []
    remaining_usernames = TIKTOK_USERNAMES.copy()
    random.shuffle(remaining_usernames)

    while len(selected_profiles) < TARGET_PROFILE_COUNT and remaining_usernames:
        username = remaining_usernames.pop()
        logger.info(f"Processing profile: {username}")

        # Publish a scrape request
        publish_scrape_request(username)

        # Wait for the pipeline to process the data
        logger.info("Waiting 30 seconds for the pipeline to process the data...")
        time.sleep(30)

        # Check the total likes
        total_likes = get_total_likes(username)
        if total_likes is None:
            logger.warning(f"Skipping {username} due to missing data.")
            continue

        if total_likes < MAX_LIKES:
            selected_profiles.append(username)
            logger.info(f"Selected {username} (total likes: {total_likes})")
        else:
            logger.info(f"Skipping {username} (total likes: {total_likes} >= {MAX_LIKES})")

    if len(selected_profiles) < TARGET_PROFILE_COUNT:
        logger.warning(f"Could only find {len(selected_profiles)} profiles with likes < {MAX_LIKES}")
    else:
        logger.info(f"Successfully selected {TARGET_PROFILE_COUNT} profiles with likes < {MAX_LIKES}")

    logger.info("Selected profiles:")
    for profile in selected_profiles:
        logger.info(f"- {profile}")

    return selected_profiles

if __name__ == "__main__":
    scrape_random_profiles()