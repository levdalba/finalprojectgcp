import functions_framework
from google.cloud import storage
import requests
import base64
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@functions_framework.cloud_event
def scrape_tiktok(cloud_event):
    try:
        data = cloud_event.data["message"]["data"]
        profile_url = base64.b64decode(data).decode("utf-8")
        logger.info(f"Scraping profile: {profile_url}")

        scrapingbee_api_key = "NWGZSCRRH5CM26REU9Q1AEQGHWJH17FYPI4UCW3FU5F3KB9W7S5PXDMK334L8JMHFBS8QLN3PD3HZIP1"
        scrapingbee_url = "https://app.scrapingbee.com/api/v1/"
        scroll_script = "window.scrollTo(0,document.body.scrollHeight);"
        scroll_script_b64 = base64.b64encode(scroll_script.encode("utf-8")).decode("utf-8")
        params = {
            "api_key": scrapingbee_api_key,
            "url": profile_url,
            "render_js": "true",
            "wait": "5000",
            "js_snippet": scroll_script_b64,
        }
        response = requests.get(scrapingbee_url, params=params)
        response.raise_for_status()
        html_content = response.text
        logger.info("Successfully fetched page using ScrapingBee with scrolling.")

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