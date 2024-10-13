import os
import json
import pandas as pd
from sqlalchemy import create_engine, types
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Fetch the database credentials from the environment variables
db_user = os.getenv('DB_USERNAME')
db_password = os.getenv('DB_PASSWORD')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')
db_name = os.getenv('DB_NAME')

# Create the connection string using the credentials from the .env file
connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(connection_string)

# Function to infer schema and handle long fields
def infer_json_schema(file_path):
    """Infers the schema from a JSON file for SQL table creation."""
    with open(file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)

        # Check if JSON data is a list of records (objects)
        if isinstance(json_data, list):
            sample_data = json_data[0]  # Use the first object for schema inference
        else:
            sample_data = json_data

        # Define column types, using TEXT for longer fields like 'message', 'media', etc.
        column_types = {}
        for col, value in sample_data.items():
            if isinstance(value, str) and len(value) > 255:  # Long strings should use TEXT
                column_types[col] = types.TEXT
            else:
                column_types[col] = types.VARCHAR(255)  # Default to VARCHAR(255) for other fields
        
        return column_types

# Load JSON files into DataFrames and write to PostgreSQL
json_files = {
    'doctorset_raw': 'C:\\Users\\1221\\Desktop\\Acadamy AIM 2\\Telegram-\\data\\raw\\DoctorsET.json',
    'eahci_raw': 'C:\\Users\\1221\\Desktop\\Acadamy AIM 2\\Telegram-\\data\\raw\\EAHCI.json',
    'yetenaweg_raw': 'C:\\Users\\1221\\Desktop\\Acadamy AIM 2\\Telegram-\\data\\raw\\yetenaweg.json'
}

for table_name, file_path in json_files.items():
    absolute_path = os.path.abspath(file_path)

    if not os.path.isfile(absolute_path):
        print(f"File not found: {absolute_path}")
        continue

    # Infer schema and read the JSON file
    try:
        schema = infer_json_schema(absolute_path)
        df = pd.read_json(absolute_path, dtype=str, encoding='utf-8')  # Ensure UTF-8 reading for Amharic

        print(f"Loaded {len(df)} records from {table_name}")

        # Write the DataFrame to PostgreSQL with the inferred schema
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=schema)
        print(f"Data successfully loaded into table '{table_name}' in PostgreSQL.")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

print("Data loading process completed.")
