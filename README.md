Below is the `README.md` file for your GitHub project:

---

# UK Crime Data Loader

This project contains a Google Cloud Function that loads UK crime data from Kaggle into Google BigQuery. The function processes multiple CSV files, cleans the data, and inserts it into specific BigQuery tables.

## Table of Contents

- [Overview](#overview)
- [Setup](#setup)
- [Environment Variables](#environment-variables)
- [BigQuery Tables](#bigquery-tables)
- [Deploying via GitHub Actions](#deploying-via-github-actions)
- [Running the Cloud Function](#running-the-cloud-function)

## Overview

This Cloud Function downloads datasets from Kaggle, processes the data, and loads it into BigQuery tables. The function handles three types of files:

1. **Stop and Search Data** (`stop-and-search.csv`)
2. **Street Crime Data** (`street.csv`)
3. **Outcomes Data** (`outcomes.csv`)

## Setup

### Prerequisites

- **Google Cloud Project**: Ensure you have a GCP project set up.
- **Kaggle API Key**: You need an API key from Kaggle to download datasets.
- **Google Cloud SDK**: Install the Google Cloud SDK for deploying the function and managing resources.
- **GitHub Repository**: Create a repository to host this project.

### Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/ajaysasane8/uk-crime-prediction.git
   cd uk-crime-prediction
   ```

2. Set up your Python environment:

   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. Install the necessary Google Cloud components:

   ```bash
   gcloud components install beta
   ```

### Environment Variables

This function retrieves the Kaggle API credentials from Google Cloud Secret Manager. You will need to create two secrets:

1. **KAGGLE_USERNAME**: Your Kaggle username.
2. **KAGGLE_KEY**: Your Kaggle API key.

You can create these secrets in the GCP Console under Secret Manager.

### BigQuery Tables

The function loads data into three BigQuery tables. Ensure these tables are created with the correct schema before deploying the function:

1. **stg_stop_search_force**
2. **stg_street_crimes**
3. **stg_outcomes**

Refer to the [BigQuery DDL section](#bigquery-ddl) for the exact table schemas.

### Deploying via GitHub Actions

This project includes a GitHub Actions workflow that automatically deploys the Cloud Function whenever you push to the `main` branch.

1. Add the following GitHub secrets:
   - **`GCP_CREDENTIALS`**: The JSON key file content of your Google Cloud service account.
   - **`GCP_PROJECT_ID`**: Your Google Cloud project ID.
   - **`GCP_REGION`**: The region where you want to deploy the Cloud Function.

2. The deployment process is defined in `.github/workflows/deploy.yml`. Ensure your service account has the necessary permissions to deploy the Cloud Function and access BigQuery and Secret Manager.

### Running the Cloud Function

Once deployed, the Cloud Function can be triggered via HTTP. The function will:

1. Download datasets from Kaggle based on a specific pattern (e.g., 2023 or 2024 data).
2. Clean the data to match the BigQuery schema.
3. Insert the data into the respective BigQuery tables.

To trigger the function, you can use an HTTP client like `curl`:

```bash
curl -X POST https://REGION-PROJECT_ID.cloudfunctions.net/load_data_to_bigquery
```

### BigQuery DDL

Here are the table schemas for the BigQuery tables:

#### Stop and Search Table

```sql
CREATE OR REPLACE TABLE `uk-crime-analysis.crimes.stg_stop_search_force` (
  `type` STRING,
  `date` DATE,
  `gender` STRING,
  `age_range` STRING,
  `self_defined_ethnicity` STRING,
  `officer_defined_ethnicity` STRING,
  `legislation` STRING,
  `object_of_search` STRING,
  `outcome` STRING,
  `outcome_linked_to_object_of_search` BOOLEAN,
  `removal_of_more_than_outer_clothing` BOOLEAN,
  `Part_of_a_policing_operation` STRING,
  `Policing_operation` STRING,
  `latitude` FLOAT64,
  `longitude` FLOAT64,
  `FILENAME` STRING
)
PARTITION BY date;
```

#### Street Crimes Table

```sql
CREATE OR REPLACE TABLE `uk-crime-analysis.crimes.stg_street_crimes` (
  `crime_id` STRING,
  `month` DATE,
  `reported_by` STRING,
  `falls_within` STRING,
  `longitude` FLOAT64,
  `latitude` FLOAT64,
  `location` STRING,
  `lsoa_code` STRING,
  `lsoa_name` STRING,
  `crime_type` STRING,
  `last_outcome_category` STRING,
  `context` STRING,
  `FILENAME` STRING
)
PARTITION BY month;
```

#### Outcomes Table

```sql
CREATE OR REPLACE TABLE `uk-crime-analysis.crimes.stg_outcomes` (
  `crime_id` STRING,
  `month` DATE,
  `reported_by` STRING,
  `falls_within` STRING,
  `longitude` FLOAT64,
  `latitude` FLOAT64,
  `location` STRING,
  `lsoa_code` STRING,
  `lsoa_name` STRING,
  `outcome_type` STRING,
  `FILENAME` STRING
)
PARTITION BY month;
```

### Troubleshooting

- **Table Not Found**: If you encounter a "Table not found" error, verify that the tables exist in BigQuery and that the project ID is correctly set.
- **Permissions Error**: Ensure that your service account has the necessary permissions to access BigQuery, Secret Manager, and deploy Cloud Functions.