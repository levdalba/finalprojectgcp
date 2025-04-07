from google.cloud import pubsub_v1
import base64
import time

# List of TikTok usernames to scrape
usernames = [
    "mrbeast",
    "laurenkettering",
    "jasonmoments",
    "charlidamelio",
    "addisonre",
    # Add more usernames here (up to 45 or as needed)
]

# Initialize the Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('training-triggering-pipeline', 'scrape-tiktok-topic')

# Publish a message for each username
for username in usernames:
    url = f"https://www.tiktok.com/@{username}"
    message_data = base64.b64encode(url.encode('utf-8'))
    try:
        future = publisher.publish(topic_path, message_data)
        message_id = future.result()  # Wait for the publish to complete
        print(f"Published message for {url}, message ID: {message_id}")
    except Exception as e:
        print(f"Error publishing message for {url}: {str(e)}")
    time.sleep(1)  # Add a small delay to avoid rate limiting

print("Finished publishing messages for all usernames.")