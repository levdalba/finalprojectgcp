import functions_framework
from google.cloud import storage
import requests
import base64
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@functions_framework.cloud_event
def scrape_tiktok(cloud_event):
    try:
        # Extract the TikTok profile URL from the Pub/Sub message
        data = cloud_event.data["message"]["data"]
        profile_url = base64.b64decode(data).decode("utf-8")
        logger.info(f"Scraping profile: {profile_url}")

        # Use ScrapingBee to fetch the page
        scrapingbee_api_key = "NWGZSCRRH5CM26REU9Q1AEQGHWJH17FYPI4UCW3FU5F3KB9W7S5PXDMK334L8JMHFBS8QLN3PD3HZIP1"  # Replace with your key!
        scrapingbee_url = "https://app.scrapingbee.com/api/v1/"
        params = {
            "api_key": scrapingbee_api_key,
            "url": profile_url,
            "render_js": "true",  # Ensures TikTok’s JavaScript renders
            "wait": "2000",       # Wait 2 seconds for full page load
        }
        response = requests.get(scrapingbee_url, params=params)
        response.raise_for_status()  # Throw an error if the request fails
        html_content = response.text
        logger.info("Successfully fetched page using ScrapingBee.")

        # Save the HTML to Google Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket("tiktok-raw-data")
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        username = profile_url.split('@')[-1]
        blob_path = f"profiles/{username}/{timestamp}.html"
        blob = bucket.blob(blob_path)
        blob.upload_from_string(html_content, content_type="text/html")
        logger.info(f"Saved raw HTML to gs://tiktok-raw-data/{blob_path}")
    except Exception as e:
        logger.error(f"Error in scrape_tiktok: {str(e)}")
        raise