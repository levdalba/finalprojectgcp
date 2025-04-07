import functions_framework
import requests
import os
from google.cloud import storage
import logging
import base64
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use the correct ScrapingBee API Key
SCRAPINGBEE_API_KEY = "TSUO2QQMLZEDIXGZCM2P31DGAGT3YST24ZC91GK85HQAG4DHQTFRWMJMOLF13A7AMHXLY69WLNFFNXSV"

def scrape_with_scrapingbee(url):
    """Scrape a URL using ScrapingBee."""
    # Define the JavaScript scenario to scroll and wait
    js_scenario = {
        "instructions": [
            {"scroll_y": 1000},
            {"wait": 1000},
            {"scroll_y": 1000}
        ]
    }
    
    scrapingbee_url = (
        "https://app.scrapingbee.com/api/v1/"
        f"?api_key={SCRAPINGBEE_API_KEY}"
        f"&url={url}"
        "&render_js=True"
        "&block_resources=False"
        "&wait=15000"  # Keep a 15-second wait for stability
        "&premium_proxy=True"
        "&js_scenario=" + json.dumps(js_scenario).replace(" ", "")
    )
    logger.info(f"Sending request to ScrapingBee with URL: {scrapingbee_url}")
    try:
        response = requests.get(scrapingbee_url)
        response.raise_for_status()
        logger.info("Successfully fetched page using ScrapingBee.")
        return response.text
    except requests.RequestException as e:
        logger.error(f"ScrapingBee request failed: {e}")
        raise

@functions_framework.cloud_event
def scrape_tiktok(cloud_event):
    """Cloud Function to scrape TikTok profiles and save HTML to GCS."""
    try:
        # Extract the URL from the Pub/Sub message
        pubsub_message = cloud_event.data["message"]
        if "data" not in pubsub_message:
            raise ValueError("No data in Pub/Sub message")
        
        url = base64.b64decode(pubsub_message["data"]).decode("utf-8")
        logger.info(f"Received URL to scrape: {url}")

        # Validate URL
        if not url.startswith("https://www.tiktok.com/@"):
            raise ValueError(f"Invalid TikTok URL: {url}")

        # Extract username from URL
        username = url.split("@")[1].split("?")[0] if "?" in url else url.split("@")[1]
        logger.info(f"Extracted username: {username}")

        # Scrape the page
        html_content = scrape_with_scrapingbee(url)
        logger.info(f"Scraped HTML content, length: {len(html_content)} characters")

        # Upload to GCS
        storage_client = storage.Client()
        bucket_name = "tiktok-raw-data"
        bucket = storage_client.bucket(bucket_name)
        blob_path = f"profiles/{username}/{username}.html"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(html_content, content_type="text/html")
        logger.info(f"Uploaded HTML to gs://{bucket_name}/{blob_path}")

        return f"Successfully scraped and uploaded {url}"

    except Exception as e:
        logger.error(f"Error in scrape_tiktok: {e}")
        raise