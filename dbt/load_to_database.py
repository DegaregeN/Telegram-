import pandas as pd
from sqlalchemy import create_engine, types
import json
import os

# Database connection
engine = create_engine('postgresql://postgres:Dna%40607625@localhost:5432/Telegram_data')

# Print the current working directory
print("Current working directory:", os.getcwd())

# Paths to JSON files
json_files = {
    'doctorset_raw': os.path.join('C:\\Users\\1221\\Desktop\\Acadamy AIM 2\\Telegram-', 'data', 'raw', 'DoctorsET.json'),
    'eahci_raw': os.path.join('C:\\Users\\1221\\Desktop\\Acadamy AIM 2\\Telegram-', 'data', 'raw', 'EAHCI.json'),
    'yetenaweg_raw': os.path.join('C:\\Users\\1221\\Desktop\\Acadamy AIM 2\\Telegram-', 'data', 'raw', 'yetenaweg.json')
}

# Function to read JSON file and infer schema
def infer_json_schema(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
        if isinstance(json_data, list):  # Check if the JSON data is a list (array of objects)
            # Assuming each object in the array has the same structure, use the first object for schema inference
            sample_data = json_data[0]
        else:
            sample_data = json_data

        column_types = {}
        for col in sample_data.keys():
            # Check if the values in the column are iterable and handle accordingly
            if isinstance(sample_data[col], (list, str)):
                max_length = max(len(str(item)) for item in sample_data[col]) if sample_data[col] else 0
                column_types[col] = types.VARCHAR(length=max_length)  # Infer VARCHAR length based on max length
            elif isinstance(sample_data[col], (int, float)):
                column_types[col] = types.FLOAT()  # Handle numeric types
            else:
                column_types[col] = types.TEXT()  # Fallback for non-string and non-numeric types

        return column_types

# Load JSON files into DataFrames and write to PostgreSQL
for table_name, file_path in json_files.items():
    # Check if the file exists
    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}")
        continue

    # Infer schema
    schema = infer_json_schema(file_path)

    # Load JSON into DataFrame
    try:
        df = pd.read_json(file_path, lines=True)  
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=schema)
        print(f"Loaded {len(df)} records into {table_name}.")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

print("Data loaded successfully into PostgreSQL.")
