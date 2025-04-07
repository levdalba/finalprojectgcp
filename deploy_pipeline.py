import zipfile
import tempfile
from google.cloud import storage
import subprocess
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = 'training-triggering-pipeline'
REGION = 'us-central1'
SCRAPINGBEE_API_KEY = "TSUO2QQMLZEDIXGZCM2P31DGAGT3YST24ZC91GK85HQAG4DHQTFRWMJMOLF13A7AMHXLY69WLNFFNXSV"

def delete_cloud_function(function_name):
    """Delete a Cloud Function if it exists."""
    try:
        delete_command = (
            f"gcloud functions delete {function_name} "
            f"--project {PROJECT_ID} "
            f"--region {REGION} "
            f"--gen2 "
            "--quiet"
        )
        subprocess.run(delete_command, shell=True, check=True)
        logger.info(f"Deleted Cloud Function: {function_name}")
    except subprocess.CalledProcessError as e:
        logger.warning(f"Function {function_name} may not exist: {e}")

def deploy_cloud_function(function_name, directory, trigger_event, trigger_resource):
    """Deploy a Cloud Function with source code uploaded to Cloud Storage."""
    if not os.path.exists(directory):
        raise Exception(f"Directory {directory} does not exist.")
    
    # Delete the existing function first
    delete_cloud_function(function_name)

    # Create a temporary zip file of the source code
    with tempfile.TemporaryDirectory() as temp_dir:
        zip_path = os.path.join(temp_dir, f"{function_name}.zip")
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for root, _, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, directory)
                    zipf.write(file_path, arcname)

        # Upload the zip file to Cloud Storage
        storage_client = storage.Client()
        source_bucket_name = "training-triggering-pipeline-source"
        source_bucket = storage_client.bucket(source_bucket_name)
        blob_name = f"{function_name}/{function_name}.zip"
        blob = source_bucket.blob(blob_name)
        blob.upload_from_filename(zip_path)
        logger.info(f"Uploaded source code to gs://{source_bucket_name}/{blob_name}")

        # Deploy the Cloud Function
        deploy_command = (
            f"gcloud functions deploy {function_name} "
            f"--runtime python39 "
            f"--trigger-event {trigger_event} "
            f"--trigger-resource {trigger_resource} "
            f"--project {PROJECT_ID} "
            f"--region {REGION} "
            f"--gen2 "
            f"--source gs://{source_bucket_name}/{blob_name} "
        )
        if function_name == "scrape_tiktok":
            deploy_command += f"--set-env-vars SCRAPINGBEE_API_KEY={SCRAPINGBEE_API_KEY}"
        logger.info(f"Deploying Cloud Function with command: {deploy_command}")
        result = subprocess.run(deploy_command, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            logger.error(f"Error deploying Cloud Function {function_name}: {result.stderr}")
            raise Exception(f"Failed to deploy Cloud Function {function_name}")
        logger.info(f"Cloud Function {function_name} deployed successfully.")

def main():
    """Deploy all Cloud Functions."""
    try:
        # Deploy scrape_tiktok
        deploy_cloud_function(
            function_name="scrape_tiktok",
            directory="scrape_tiktok",
            trigger_event="google.pubsub.topic.publish",
            trigger_resource="scrape-tiktok-topic"
        )

        # Deploy process_tiktok_data
        deploy_cloud_function(
            function_name="process_tiktok_data",
            directory="process_tiktok_data",
            trigger_event="google.storage.object.finalize",
            trigger_resource="tiktok-raw-data"
        )

        # Deploy monitor_pipeline
        deploy_cloud_function(
            function_name="monitor_pipeline",
            directory="monitor_pipeline",
            trigger_event="google.pubsub.topic.publish",
            trigger_resource="monitor-pipeline-topic"
        )

        logger.info("All Cloud Functions deployed successfully.")

    except Exception as e:
        logger.error(f"Deployment failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()