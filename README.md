# TikTok Data Pipeline and Analytics 📈

## 🚀 Overview
This project implements a robust data pipeline that scrapes TikTok profile and video data, processes it using Python and BeautifulSoup, and stores the results in Google BigQuery for advanced analytics. Visualization is achieved via a Looker Studio dashboard.

The objective is to extract valuable insights from TikTok creator data—such as follower counts, video views, and engagement metrics—to analyze performance and uncover trends.

## 🔧 Tech Stack
- **Web Scraping**: BeautifulSoup for parsing HTML
- **DOM Analysis**: Reverse-engineered DOM structure to locate and extract key TikTok data
- **Scraping Service**: ScrapingBee for web scraping
- **Cloud Infrastructure**:
  - Google Cloud Functions
  - Google Cloud Storage (GCS)
  - Google BigQuery
- **Visualization**: Looker Studio for dashboards

## 🧱 Pipeline Architecture
1. **Scraping TikTok profiles**
   - Uses ScrapingBee to fetch profile HTML
   - Extracts embedded JSON from the DOM using BeautifulSoup and manual DOM inspection

2. **Processing Data**
   - Parses raw HTML to extract:
     - Creator info
     - Video details
   - Deduplicates using MERGE in BigQuery

3. **Data Storage**
   - Raw HTML saved to GCS
   - Cleaned/structured data loaded into BigQuery

4. **Visualization**
   - Connected to BigQuery via Looker Studio

## 📊 Looker Studio Dashboard Insights
Access the full dashboard here: [TikTok Analytics Dashboard](https://lookerstudio.google.com/u/0/reporting/cff2d309-6362-4599-8962-43c3370a69d0/page/9doFF/edit)

### 👥 Followers per Creator
- Bar chart displaying follower counts across different TikTok creators
- Key insights:
  - @mrbeast leads with 115.2M followers
  - @zachking follows with 82.3M followers
  - @therock has 80.4M followers
  - Total followers across all tracked creators: 357,206,115

### ✔️ Verified vs. Non-Verified Creators
Pie chart breakdown:
- Verified creators: 54.5%
- Non-verified creators: 45.5%

### 📊 Additional Visualizations
- Scatter plot comparing follower count to like count
- Performance trends over time
- Engagement rate analysis

## 📁 Project Structure
```
tiktok-data-pipeline/
├── scrape_tiktok/           # Cloud Function to scrape and save raw HTML
│   └── main.py
├── process_tiktok_data/     # Cloud Function to parse HTML and upload to BigQuery
│   └── main.py
├── sql_scripts/             # SQL scripts for cleaning and verification
│   └── filter_invalid_profiles.sql
├── looker_studio_dashboard/ # Dashboard configuration or link
```

## 🛠️ Prerequisites
- A **Google Cloud Project** with billing enabled  
- Enabled services:
  - Cloud Functions  
  - Cloud Storage  
  - BigQuery  
  - Pub/Sub  
- **ScrapingBee API Key**  
- Access to **Looker Studio**  
- `gcloud` CLI installed and authenticated  

## ⚙️ Setup Instructions
### 1. Clone the Repo
```bash
git clone https://github.com/<your-username>/tiktok-data-pipeline.git
cd tiktok-data-pipeline
```

### 2. Create Google Cloud Resources
#### 📦 Create Storage Buckets
```bash
gsutil mb -l us-central1 gs://tiktok-raw-data
gsutil mb -l us-central1 gs://tiktok-processed-data
```

#### 🧮 Create BigQuery Dataset and Tables
```sql
CREATE SCHEMA `training-triggering-pipeline.tiktok_dataset`;

CREATE TABLE `training-triggering-pipeline.tiktok_dataset.profiles` (
  username STRING NOT NULL,
  user_id STRING,
  actual_name STRING,
  following_count INT64,
  follower_count INT64,
  total_like_count INT64,
  caption STRING,
  bio_link STRING,
  bio STRING,
  profile_pic_url STRING,
  is_verified BOOL,
  scrape_timestamp TIMESTAMP
);

CREATE TABLE `training-triggering-pipeline.tiktok_dataset.videos` (
  url STRING NOT NULL,
  views INT64,
  thumbnail STRING,
  description STRING,
  create_time STRING,
  like_count INT64,
  comment_count INT64,
  share_count INT64
);
```

#### 📣 Create Pub/Sub Topic
```bash
gcloud pubsub topics create scrape-tiktok-topic --project training-triggering-pipeline
```

### 3. Deploy Cloud Functions
#### 🕸️ scrape_tiktok – Scraping Function
```bash
cd scrape_tiktok
gcloud functions deploy scrape_tiktok \
  --runtime python39 \
  --trigger-topic scrape-tiktok-topic \
  --region us-central1 \
  --memory 256MB \
  --timeout 300s \
  --project training-triggering-pipeline \
  --no-gen2 \
  --set-env-vars SCRAPINGBEE_API_KEY=<your-scrapingbee-api-key>
```

#### 🧹 process_tiktok_data – Processing Function
```bash
cd ../process_tiktok_data
gcloud functions deploy process_tiktok_data \
  --runtime python39 \
  --trigger-event google.storage.object.finalize \
  --trigger-resource tiktok-raw-data \
  --region us-central1 \
  --memory 512MB \
  --timeout 540s \
  --project training-triggering-pipeline \
  --no-gen2
```

### 4. Trigger a Scrape
```bash
gcloud pubsub topics publish scrape-tiktok-topic \
  --message "https://www.tiktok.com/@jasonmoments" \
  --project training-triggering-pipeline
```

### 5. Monitor the Pipeline
```bash
gcloud functions logs read scrape_tiktok --limit 50
gcloud functions logs read process_tiktok_data --limit 50
```

### 6. Local Development
```bash
export SCRAPINGBEE_API_KEY="your_actual_api_key_here"
functions-framework --target=scrape_tiktok
```

### 7. Access the Dashboard
Open [Looker Studio Dashboard](https://lookerstudio.google.com/u/0/reporting/cff2d309-6362-4599-8962-43c3370a69d0/page/9doFF/edit) or connect to the BigQuery dataset:
- `training-triggering-pipeline.tiktok_dataset.profiles`
- `training-triggering-pipeline.tiktok_dataset.videos`

## 🤝 Contributing
1. Fork this repo
2. Create a new branch:
   ```bash
   git checkout -b feature/your-feature
   ```
3. Commit your changes:
   ```bash
   git commit -m "Add your feature"
   ```
4. Push to your branch:
   ```bash
   git push origin feature/your-feature
   ```
5. Open a Pull Request 🚀

## 🧠 Credits
Built with ❤️ using Python, BeautifulSoup, and Google Cloud

Author: Levan Dalbashvili
