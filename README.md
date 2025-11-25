# Forex Analytics Project

## 🚀 Project Overview
The **Forex Analytics Project** is an end-to-end pipeline designed to streamline the processing and analysis of foreign exchange and sales data. Built on **AWS serverless architecture**, it enables secure data storage, automated ingestion, efficient ETL processing, and insightful visualizations.

---

## 🏗️ Architecture and Features

### **Data Storage**
- **S3 Buckets**:
  - **`forex-raw`**: Stores raw datasets, such as Forex feed and customer sales data.
  - **`forex-processed`**: Holds curated datasets for analytics and reporting.
- **Key Features**:
  - Server-Side Encryption (SSE-KMS) for secure data storage.
  - Versioning enabled for data integrity and recovery.
  - Public access is blocked to ensure compliance and security.

---

### **Data Ingestion**
1. **Forex Conversion Rates**:
   - **Ingestion via**: AWS Lambda function.
   - **Process**:
     - Periodically fetches exchange rates.
     -  Stores data in the `forex-processed` bucket.
     - Invalid records are routed to the `forex-error` bucket.

2. **Customer Sales Data**:
   - **Ingestion via**: AWS Glue ETL job.
   - **Process**:
     - Reads raw data from the `forex-raw` bucket.
     - Applies transformations.
     - Stores results as **Parquet** and **CSV** files in the `forex-processed` bucket.

---

### **Data Processing and ETL**
- **AWS Glue Jobs**:
  - **ETL Pipeline 1**:
    - Formats customer sales data with a defined schema.
    - Outputs **Parquet** (analytics-friendly) and **CSV** (interoperability) files.
  - **ETL Pipeline 2**:
    - Merges Forex and customer datasets.
    - Implements Data Quality (DQ) checks:
      - Schema validation, completeness checks, duplicate removal.
    - Results:
      - **Valid data** → Stored in `forex-processed`.
      - **Invalid data** → Routed to `forex-error`.

---

### **Querying and Analytics**
- **AWS Athena**:
  - Enables interactive querying on curated datasets stored in the `forex-processed` bucket.
  - Key analyses:
    - Average exchange rates over time.
    - Conversion trends for local currency to EUR and USD.
    - Top 5 traded currency pairs by transaction value.

---
  
### **Visualizations**
- **Built using QuickSight**:
  - **Volume Metrics**:
    - Monitors record ingestion and processing metrics.
    - KPI indicators for performance tracking.
  - **Trend Metrics**:
    - Tracks sales and conversion trends over time.
  - **Performance Metrics**:
    - Measures exchange rate trends (daily/weekly/monthly).
    - Highlights top-performing currency pairs and transaction volumes.

---

## 🛡️ Data Security and Governance
- **Encryption**: SSE-KMS ensures secure data storage.
- **Controlled Access**: Bucket policies restrict access levels.
- **Versioning**: Provides data recovery capabilities and historical tracking.

---

## 📊 Key Achievements
- **Automated Ingestion**:
  - Data from Forex feed via Lambda functions.
  - Customer sales data via Glue ETL jobs.
- **Data Quality Assurance**:
  - Cleaned, validated datasets stored for analytics.
  - Invalid rows routed for manual review.
- **Interactive Dashboards**:
  - QuickSight visualizations for actionable business insights.

---

## 📂 Development Details
- **Programming Languages**: Python, SQL, PySpark
- **Services and Tools**:
  - **S3** for storage.
  - **AWS Lambda** for Forex data ingestion.
  - **AWS Glue** for processing and transformation.
  - **Athena** for querying datasets.
  - **QuickSight** for dashboards and insights.

---
