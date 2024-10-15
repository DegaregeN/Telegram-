import pandas as pd
from sqlalchemy import create_engine, types
import json
import os

# Database connection
engine = create_engine('postgresql://postgres:passward@localhost:5432/Telegram_data')

# Print the current working directory
print("Current working directory:", os.getcwd())

# Paths to JSON files
json_files = {
    'doctorset_raw': os.path.join('C:\\Users\\1221\\Desktop\\Acadamy AIM 2\\Telegram-\\data\\raw\\DoctorsET.json'),
    'eahci_raw': os.path.join('C:\\Users\\1221\\Desktop\\Acadamy AIM 2\\Telegram-\\data\\raw\\EAHCI.json'),
    'yetenaweg_raw': os.path.join('C:\\Users\\1221\\Desktop\\Acadamy AIM 2\\Telegram-\\data\\raw\\yetenaweg.json')
}

# Function to read JSON file and infer schema
def infer_json_schema(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
        sample_data = json_data[0] if isinstance(json_data, list) else json_data

        column_types = {}
        for col in sample_data.keys():
            if isinstance(sample_data[col], list):  # If it's a list
                max_length = max(len(str(item)) for item in sample_data[col]) if sample_data[col] else 0
                column_types[col] = types.VARCHAR(length=max_length)
            elif isinstance(sample_data[col], str):
                max_length = len(sample_data[col])
                column_types[col] = types.VARCHAR(length=max_length)
            elif isinstance(sample_data[col], (int, float)):
                column_types[col] = types.FLOAT()
            else:
                column_types[col] = types.TEXT()

        return column_types

# Load JSON files into DataFrames and write to PostgreSQL
for table_name, file_path in json_files.items():
    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}")
        continue

    schema = infer_json_schema(file_path)

    try:
        df = pd.read_json(file_path, lines=True)  # Use lines=True for newline-delimited JSON
        print(f"DataFrame for {table_name}:")
        print(df.head())  # Print the first few rows of the DataFrame

        with engine.begin() as connection:
            df.to_sql(table_name, connection, if_exists='replace', index=False, dtype=schema)
            print(f"Loaded {len(df)} records into {table_name}.")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

print("Data loading process completed.")
