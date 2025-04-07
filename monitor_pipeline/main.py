import functions_framework
from google.cloud import logging as gcp_logging
from google.cloud import pubsub_v1
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@functions_framework.http
def monitor_pipeline(request):
    """Cloud Function to monitor the pipeline for errors."""
    try:
        # Initialize the Cloud Logging client
        logging_client = gcp_logging.Client()
        logger = logging_client.logger("cloudfunctions.googleapis.com/cloud-functions")

        # Define the filter for error logs from scrape_tiktok and process_tiktok_data
        log_filter = (
            'logName:"/logs/cloudfunctions.googleapis.com%2Fcloud-functions" '
            'severity>=ERROR '
            '("scrape_tiktok" OR "process_tiktok_data") '
            '-"monitor_pipeline"'
        )

        # Fetch recent error logs
        errors = []
        for entry in logger.list_entries(filter_=log_filter, page_size=10):
            errors.append({
                "timestamp": entry.timestamp.isoformat(),
                "function": entry.resource.labels.get("function_name", "unknown"),
                "message": entry.payload
            })

        if errors:
            logger.warning(f"Found {len(errors)} errors in the pipeline")
            # Publish errors to a Pub/Sub topic for alerting
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path("training-triggering-pipeline", "pipeline-errors")
            for error in errors:
                message = f"Error in {error['function']} at {error['timestamp']}: {error['message']}"
                publisher.publish(topic_path, message.encode("utf-8"))
                logger.info(f"Published error to Pub/Sub: {message}")
        else:
            logger.info("No errors found in the pipeline")

        return {"status": "success", "errors_found": len(errors)}

    except Exception as e:
        logger.error(f"Error in monitor_pipeline: {str(e)}")
        return {"status": "error", "message": str(e)}, 500