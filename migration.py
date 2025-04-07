from google.cloud import bigquery

# Configuration
PROJECT_ID = 'training-triggering-pipeline'
DATASET_ID = 'tiktok_dataset'
TABLE_ID = 'profile_video_summary'
TEMP_TABLE_ID = 'profile_video_summary_temp'

# Desired schema
DESIRED_SCHEMA = [
    bigquery.SchemaField("username", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("follower_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("total_like_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("total_views", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("latest_scrape", "TIMESTAMP", mode="NULLABLE"),
]

# Initialize BigQuery client
client = bigquery.Client(project=PROJECT_ID)

def migrate_table():
    """Migrate the existing table to the new schema."""
    # Step 1: Create a temporary table with the desired schema
    temp_table_ref = client.dataset(DATASET_ID).table(TEMP_TABLE_ID)
    temp_table = bigquery.Table(temp_table_ref, schema=DESIRED_SCHEMA)
    client.create_table(temp_table, exists_ok=True)
    print(f"Temporary table {TEMP_TABLE_ID} created with the desired schema.")

    # Step 2: Migrate data from the existing table to the temporary table
    # - Cast total_like_count from STRING to INTEGER
    # - Ensure username is not NULL (filter out NULL usernames)
    migration_query = f"""
    INSERT INTO `{PROJECT_ID}.{DATASET_ID}.{TEMP_TABLE_ID}`
    (username, follower_count, total_like_count, video_count, total_views, latest_scrape)
    SELECT
        username,
        follower_count,
        SAFE_CAST(total_like_count AS INTEGER) AS total_like_count,
        video_count,
        total_views,
        latest_scrape
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE username IS NOT NULL
    """
    migration_job = client.query(migration_query)
    migration_job.result()
    print(f"Data migrated to temporary table {TEMP_TABLE_ID}.")

    # Step 3: Delete the existing table
    client.delete_table(f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", not_found_ok=True)
    print(f"Original table {TABLE_ID} deleted.")

    # Step 4: Rename the temporary table to the original table name
    rename_query = f"""
    ALTER TABLE `{PROJECT_ID}.{DATASET_ID}.{TEMP_TABLE_ID}`
    RENAME TO {TABLE_ID}
    """
    rename_job = client.query(rename_query)
    rename_job.result()
    print(f"Temporary table {TEMP_TABLE_ID} renamed to {TABLE_ID}.")

def verify_schema():
    """Verify the schema of the table after migration."""
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    table = client.get_table(table_ref)
    print("Table schema after migration:")
    for field in table.schema:
        print(f"Field: {field.name}, Type: {field.field_type}, Mode: {field.mode}")

if __name__ == "__main__":
    # Perform the migration
    migrate_table()
    
    # Verify the schema
    verify_schema()