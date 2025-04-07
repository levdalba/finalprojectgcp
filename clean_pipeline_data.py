from google.cloud import bigquery
from google.cloud import storage
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = 'training-triggering-pipeline'
DATASET_ID = 'tiktok_dataset'
RAW_BUCKET_NAME = 'tiktok-raw-data'
PROCESSED_BUCKET_NAME = 'tiktok-processed-data'

def delete_bigquery_data():
    """Delete all data from the profiles and videos tables in BigQuery."""
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # Delete from profiles table
    profiles_table_id = f"{PROJECT_ID}.{DATASET_ID}.profiles"
    delete_profiles_query = f"DELETE FROM `{profiles_table_id}` WHERE TRUE"
    logger.info(f"Deleting all data from {profiles_table_id}...")
    bq_client.query(delete_profiles_query).result()
    logger.info(f"Deleted all data from {profiles_table_id}.")

    # Delete from videos table
    videos_table_id = f"{PROJECT_ID}.{DATASET_ID}.videos"
    delete_videos_query = f"DELETE FROM `{videos_table_id}` WHERE TRUE"
    logger.info(f"Deleting all data from {videos_table_id}...")
    bq_client.query(delete_videos_query).result()
    logger.info(f"Deleted all data from {videos_table_id}.")

def delete_gcs_data(bucket_name):
    """Delete all objects in the specified GCS bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()
    
    logger.info(f"Deleting all objects in gs://{bucket_name}...")
    count = 0
    for blob in blobs:
        blob.delete()
        count += 1
    logger.info(f"Deleted {count} objects from gs://{bucket_name}.")

def clean_pipeline_data():
    """Clean all data in the TikTok scraping pipeline."""
    try:
        # Delete BigQuery data
        delete_bigquery_data()
        
        # Delete GCS data
        delete_gcs_data(RAW_BUCKET_NAME)
        delete_gcs_data(PROCESSED_BUCKET_NAME)
        
        logger.info("Pipeline data cleaning completed successfully.")
    except Exception as e:
        logger.error(f"Error cleaning pipeline data: {str(e)}")
        raise

if __name__ == "__main__":
    clean_pipeline_data()