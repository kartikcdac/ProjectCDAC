#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 22 09:08:37 2024

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
path = "Datasets/Lookup"

# GitHub API URL to list files in the Product schema
api_url = f"https://api.github.com/repos/kartikcdac/ProjectCDAC/contents/Datasets/Lookup?ref=main"

response = requests.get(api_url)
files = response.json()

if not isinstance(files, list):
    raise ValueError("Error fetching files. Response is not a list.")

dataframes = {}

primary_keys = {
    "Lookup.Territory": ["SalesTerritoryKey"],
    "Lookup.Customer": ["CustomerKey"],
    "Lookup.Calender": ["Date"],
}

for file in files:
    if file['name'].endswith('.csv'):
        csv_url = file['download_url']
        csv_response = requests.get(csv_url)
        
        if csv_response.status_code == 200:
            df = pd.read_csv(StringIO(csv_response.text))
            df.dropna(inplace=True)
            
            file_name = file['name'].replace('.csv', '')
            schema_name, table_name = file_name.split('.')
            
            # Ensure that the primary key exists in the dictionary
            primary_key_columns = primary_keys.get(f"{schema_name}.{table_name}")
            if primary_key_columns:
                df.drop_duplicates(subset=primary_key_columns, inplace=True)
            else:
                print(f"No primary key defined for {schema_name}.{table_name}. Skipping duplicate check.")
            
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

for table_name, df in dataframes.items():
    try:
        df.to_sql(table_name, engine, schema=schema_name, if_exists='append', index=False, chunksize=1000)
        print(f"DataFrame {table_name} uploaded to PostgreSQL successfully.")
    except Exception as e:
        print(f"Error uploading {table_name}: {e}")

engine.dispose()
