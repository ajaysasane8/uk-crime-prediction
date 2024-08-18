import os
import tempfile
import kaggle
import zipfile
import pandas as pd
from google.cloud import bigquery
from google.cloud import secretmanager
import functions_framework
import datetime
import time
from google.api_core.exceptions import NotFound

# Set your BigQuery project and dataset details
PROJECT_ID = 'uk-crime-analysis'
DATASET_ID = 'crimes'
STOP_SEARCH_TABLE_ID = 'stg_stop_search_force'
STREET_CRIMES_TABLE_ID = 'stg_street_crimes'
OUTCOMES_TABLE_ID = 'stg_outcomes'

# Set your Kaggle dataset details
KAGGLE_DATASET = 'mexwell/uk-police-data'


def access_secret_version(secret_id):
    """Access the payload for the given secret version."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")


def download_kaggle_dataset(temp_dir):
    """Download the Kaggle dataset and extract files matching the target patterns."""
    kaggle_username = access_secret_version('KAGGLE_USERNAME')
    kaggle_key = access_secret_version('KAGGLE_KEY')

    kaggle.api.authenticate()

    kaggle.api.dataset_download_files(KAGGLE_DATASET, path=temp_dir, unzip=False)

    zip_file = None
    for filename in os.listdir(temp_dir):
        if filename.endswith(".zip"):
            zip_file = os.path.join(temp_dir, filename)
            break

    if not zip_file:
        raise Exception("No zip file found in the downloaded dataset.")

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)

    files = []
    for root, dirs, filenames in os.walk(temp_dir):
        for filename in filenames:
            if ((filename.startswith('2023') or filename.startswith('2024')) and
                    (filename.endswith('stop-and-search.csv') or filename.endswith('street.csv') or filename.endswith(
                        'outcomes.csv'))):
                file_path = os.path.join(root, filename)
                if os.path.exists(file_path):
                    files.append(file_path)
                else:
                    print(f"File path does not exist: {file_path}")

    if not files:
        raise Exception("No files found matching the pattern for years 2023 and 2024.")

    return files


def clean_data_stop_search(df, filename):
    """Replace NaN and ensure all values are JSON serializable. Also, map columns to match BigQuery schema and add filename."""
    df.rename(columns={
        'Type': 'type',
        'Date': 'date',
        'Gender': 'gender',
        'Age range': 'age_range',
        'Self-defined ethnicity': 'self_defined_ethnicity',
        'Officer-defined ethnicity': 'officer_defined_ethnicity',
        'Legislation': 'legislation',
        'Object of search': 'object_of_search',
        'Outcome': 'outcome',
        'Outcome linked to object of search': 'outcome_linked_to_object_of_search',
        'Removal of more than just outer clothing': 'removal_of_more_than_outer_clothing',
        'Part of a policing operation': 'Part_of_a_policing_operation',
        'Policing operation': 'Policing_operation',
    }, inplace=True)

    df = df.where(pd.notnull(df), None)
    df = df.astype(object).where(pd.notnull(df), None)

    df['date'] = df['date'].apply(lambda x: x.split('T')[0] if isinstance(x, str) else x)
    df['FILENAME'] = filename

    records = df.to_dict(orient='records')

    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    return records


def clean_data_street_crimes(df, filename):
    """Replace NaN and ensure all values are JSON serializable. Also, map columns to match BigQuery schema and add filename."""
    df.rename(columns={
        'Crime ID': 'crime_id',
        'Month': 'month',
        'Reported by': 'reported_by',
        'Falls within': 'falls_within',
        'Longitude': 'longitude',
        'Latitude': 'latitude',
        'Location': 'location',
        'LSOA code': 'lsoa_code',
        'LSOA name': 'lsoa_name',
        'Crime type': 'crime_type',
        'Last outcome category': 'last_outcome_category',
        'Context': 'context',
    }, inplace=True)

    df = df.where(pd.notnull(df), None)
    df = df.astype(object).where(pd.notnull(df), None)

    df['month'] = df['month'].apply(lambda x: pd.to_datetime(x, format='%Y-%m').date())
    df['month'] = df['month'].apply(lambda x: x.isoformat() if isinstance(x, datetime.date) else x)

    df['FILENAME'] = filename

    records = df.to_dict(orient='records')

    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    return records


def clean_data_outcomes(df, filename):
    """Replace NaN and ensure all values are JSON serializable. Also, map columns to match BigQuery schema and add filename."""
    df.rename(columns={
        'Crime ID': 'crime_id',
        'Month': 'month',
        'Reported by': 'reported_by',
        'Falls within': 'falls_within',
        'Longitude': 'longitude',
        'Latitude': 'latitude',
        'Location': 'location',
        'LSOA code': 'lsoa_code',
        'LSOA name': 'lsoa_name',
        'Outcome type': 'outcome_type',
    }, inplace=True)

    df = df.where(pd.notnull(df), None)
    df = df.astype(object).where(pd.notnull(df), None)

    df['month'] = df['month'].apply(lambda x: pd.to_datetime(x, format='%Y-%m').date())
    df['month'] = df['month'].apply(lambda x: x.isoformat() if isinstance(x, datetime.date) else x)

    df['FILENAME'] = filename

    records = df.to_dict(orient='records')

    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None

    return records


def insert_rows_with_retry(client, table_id, rows_to_insert, max_retries=3):
    """Insert rows into BigQuery table with retry logic for handling intermittent errors."""
    for attempt in range(max_retries):
        try:
            errors = client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                print(f"Errors during insert: {errors}")
                if any(error['reason'] == 'notFound' for error in errors[0]['errors']):
                    raise NotFound(f"Table {table_id} not found.")
                raise Exception(f"Failed to insert rows: {errors}")
            print(f"Inserted {len(rows_to_insert)} rows into {table_id}")
            break  # Break if insert is successful
        except NotFound as e:
            print(f"Attempt {attempt + 1} failed with error: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in 5 seconds...")
                time.sleep(5)
            else:
                raise
        except Exception as e:
            print(f"Attempt {attempt + 1} failed with error: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in 5 seconds...")
                time.sleep(5)
            else:
                raise


def load_data_into_bigquery(files):
    client = bigquery.Client(project=PROJECT_ID)  # Ensure project ID is set here

    for file in files:
        if not os.path.exists(file):
            print(f"ERROR: File not found at {file}")
            continue

        df = pd.read_csv(file)
        filename = os.path.basename(file)

        if filename.endswith('stop-and-search.csv'):
            records = clean_data_stop_search(df, filename)
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{STOP_SEARCH_TABLE_ID}"
        elif filename.endswith('street.csv'):
            records = clean_data_street_crimes(df, filename)
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{STREET_CRIMES_TABLE_ID}"
        elif filename.endswith('outcomes.csv'):
            records = clean_data_outcomes(df, filename)
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{OUTCOMES_TABLE_ID}"
        else:
            continue

        rows_to_insert = []
        batch_size = 10000

        for record in records:
            rows_to_insert.append(record)
            if len(rows_to_insert) >= batch_size:
                insert_rows_with_retry(client, table_id, rows_to_insert)
                rows_to_insert = []  # Clear the batch

        if rows_to_insert:
            insert_rows_with_retry(client, table_id, rows_to_insert)

    return f"Successfully inserted all rows into BigQuery."


@functions_framework.http
def load_data_to_bigquery(request):
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            files = download_kaggle_dataset(temp_dir)

            if not files:
                return "No files found matching the pattern", 200

            result = load_data_into_bigquery(files)

        return result, 200

    except Exception as e:
        return str(e), 500
