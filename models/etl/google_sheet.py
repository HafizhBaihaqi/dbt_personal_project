'''
This is an ETL script to read and extract data from Google Sheet and load it to BigQuery as a Table
'''

# Import necessary libraries
# Don't forget to install the libraries using "pip install libraryName"
import pandas as pd
from google.cloud import bigquery
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime

# BigQuery client
def bq_client():
    return bigquery.Client()

# Extracting the Google Sheet
def extract_sheet():
    # API
    ## To enable the API service: Google Cloud -> APIs -> Library
    ## Credentials can be fetched from Google Cloud -> APIs -> Credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        "service-account-key.json", 
        ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(credentials)
    
    # The sheet can be called using name of the Sheet or URL
    sheet = gc.open("Sheet Name").tabname 
    records = sheet.get_all_records()
    
    # Convert to a Data Frame
    df_sheet = pd.DataFrame(records)
    
    return df_sheet

# Transform the data
def transform_data(data):
    # Add a timestamp to flag when was the ETL script was executed
    data["loaded_at"] = datetime.utcnow()
    
    # Standardize column names
    standardized_columns = {
        "column 1": "column_1",
        "column 2": "column_2",
        "column 3": "column_3",
        "column 4": "column_4",
        "loaded_at": "loaded_at"
    }
    data = data.rename(columns=standardized_columns)

    return data

# Load data into BigQuery table
def load_to_staging_table(dataframe):
    client = bq_client()
    table_id = "project_id.dataset_id.stg_model_name"
    
    # Define table schema
    ## Write append will write the new data to the table everytime the ETL script is executed
    ## without overwrite existing data
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("column_1", "INTEGER"),
            bigquery.SchemaField("column_2", "INTEGER"),
            bigquery.SchemaField("column_3", "INTEGER"),
            bigquery.SchemaField("column_4", "STRING"),
            bigquery.SchemaField("loaded_at", "DATETIME"),
        ],
        write_disposition="WRITE_APPEND"
    )
    
    # Load data into BigQuery
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    job.result()

# Main ETL Process
def etl_process():
    # Extract
    df_sheet = extract_sheet()
    
    # Transform
    df_transformed = transform_data(df_sheet)
    
    # Load
    load_to_staging_table(df_transformed)

etl_process()