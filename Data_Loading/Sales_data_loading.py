#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 22 08:47:35 2024

@author: kartikjindal
"""

import requests
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine

# GitHub repository details
owner = "kartikcdac"
repo = "ProjectCDAC"
branch = "main"
path = "Datasets/Sales"

# GitHub API URL to list files in the Sales schema
api_url = f"https://api.github.com/repos/kartikcdac/ProjectCDAC/contents/Datasets/Sales?ref=main"

# Make a GET request to the GitHub API
response = requests.get(api_url)
files = response.json()

# Check if response is valid
if not isinstance(files, list):
    raise ValueError("Error fetching files. Response is not a list.")

# Initialize an empty dictionary to store DataFrames
dataframes = {}

primary_keys = {
    "Sales": ["OrderNumber"],
}

# Loop through each file in the repository
for file in files:
    if file['name'].endswith('.csv'):
        csv_url = file['download_url']
        csv_response = requests.get(csv_url)
        
        if csv_response.status_code == 200:
            df = pd.read_csv(StringIO(csv_response.text))
            df.dropna(inplace=True)
            
            # Debug: Print the column names
            print(f"Columns in {file['name']}: {df.columns.tolist()}")

            # Check if the primary key columns exist in the DataFrame
            file_name = file['name'].replace('.csv', '')
            schema_name, table_name = "Sales", "Sales"
            primary_key_columns = primary_keys.get(schema_name, [])

            # Handle potential column name issues (e.g., case sensitivity, extra spaces)
            df.columns = df.columns.str.strip()  # Strip any extra spaces

            # Ensure that the primary key columns exist
            missing_cols = [col for col in primary_key_columns if col not in df.columns]
            if missing_cols:
                raise KeyError(f"Missing primary key columns in {file['name']}: {missing_cols}")

            # Remove duplicates based on primary key
            df.drop_duplicates(subset=primary_key_columns, inplace=True)
            
            dataframes[table_name] = df
            print(f"Loaded and cleaned data from {file['name']} into DataFrame.")
        else:
            print(f"Failed to download {file['name']}.")

# PostgreSQL connection details
db_user = 'postgres'
db_password = 'Karjin545cdac'
db_host = 'localhost'
db_port = '5433'
db_name = 'ware_test'

# Create the PostgreSQL engine
engine = create_engine(f'postgresql+psycopg2://postgres:Karjin545cdac@localhost:5433/ware_test')

# Upload each DataFrame to PostgreSQL as a table
for table_name, df in dataframes.items():
    try:
        df.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False, chunksize=1000)
        print(f"DataFrame {table_name} uploaded to PostgreSQL successfully.")
    except Exception as e:
        print(f"Error uploading {table_name}: {e}")

engine.dispose()
