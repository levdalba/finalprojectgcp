from google.cloud import bigquery

# Configuration
PROJECT_ID = 'training-triggering-pipeline'
DATASET_ID = 'tiktok_dataset'
VIEW_TABLE_ID = 'profile_video_summary'

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

def update_view():
    """Delete the existing view and create a new one with the updated query."""
    # Step 1: Delete the existing view (if it exists)
    view_ref = client.dataset(DATASET_ID).table(VIEW_TABLE_ID)
    client.delete_table(view_ref, not_found_ok=True)
    print(f"Existing view {VIEW_TABLE_ID} deleted (if it existed).")

    # Step 2: Define the updated SQL query for the view
    view_query = """
    SELECT
        p.username AS username,
        MAX(p.follower_count) AS follower_count,
        SAFE_CAST(MAX(p.total_like_count) AS INTEGER) AS total_like_count,
        COUNT(v.url) AS video_count,
        COALESCE(SUM(v.views), 0) AS total_views,
        GREATEST(
            MAX(p.scrape_timestamp),
            COALESCE(MAX(v.scrape_timestamp), '1970-01-01 00:00:00 UTC')
        ) AS latest_scrape
    FROM `training-triggering-pipeline.tiktok_dataset.profiles` p
    LEFT JOIN `training-triggering-pipeline.tiktok_dataset.videos` v
        ON v.url LIKE CONCAT('%@', p.username, '%')
    WHERE p.username IS NOT NULL
    GROUP BY p.username
    """

    # Step 3: Create the view
    view = bigquery.Table(view_ref)
    view.view_query = view_query
    view = client.create_table(view)  # Creates the view
    print(f"View {VIEW_TABLE_ID} updated successfully.")

def verify_schema():
    """Verify the schema of the view."""
    view_ref = client.dataset(DATASET_ID).table(VIEW_TABLE_ID)
    view = client.get_table(view_ref)
    print("View schema:")
    for field in view.schema:
        print(f"Field: {field.name}, Type: {field.field_type}, Mode: {field.mode}")

if __name__ == "__main__":
    # Update the view
    update_view()

    # Verify the schema
    verify_schema()