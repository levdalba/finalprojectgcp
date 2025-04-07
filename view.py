from google.cloud import bigquery

# Configuration
PROJECT_ID = 'training-triggering-pipeline'
DATASET_ID = 'tiktok_dataset'
VIEW_TABLE_ID = 'profile_video_summary'

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

def create_view():
    """Delete the existing table and create a view for profile_video_summary."""
    # Step 1: Delete the existing table (if it exists)
    view_ref = client.dataset(DATASET_ID).table(VIEW_TABLE_ID)
    client.delete_table(view_ref, not_found_ok=True)
    print(f"Existing table/view {VIEW_TABLE_ID} deleted (if it existed).")

    # Step 2: Define the SQL query for the view
    view_query = """
    SELECT
        p.username AS username,
        p.follower_count AS follower_count,
        SAFE_CAST(p.total_like_count AS INTEGER) AS total_like_count,
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
    GROUP BY p.username, p.follower_count, p.total_like_count
    """

    # Step 3: Create the view
    view = bigquery.Table(view_ref)
    view.view_query = view_query
    view = client.create_table(view)  # Creates the view
    print(f"View {VIEW_TABLE_ID} created successfully.")

def verify_schema():
    """Verify the schema of the view."""
    view_ref = client.dataset(DATASET_ID).table(VIEW_TABLE_ID)
    view = client.get_table(view_ref)
    print("View schema:")
    for field in view.schema:
        print(f"Field: {field.name}, Type: {field.field_type}, Mode: {field.mode}")

if __name__ == "__main__":
    # Create the view
    create_view()

    # Verify the schema
    verify_schema()