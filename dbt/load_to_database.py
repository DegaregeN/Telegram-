import pandas as pd
from sqlalchemy import create_engine, types
import json
import os

# Database connection (Update the credentials and database name as needed)
engine = create_engine('postgresql://postgres:admin@localhost:5432/Data-warehouse')

# Print the current working directory to verify paths
print("Current working directory:", os.getcwd())

# Corrected paths to JSON files (Use relative paths if files are in a different folder)
json_files = {
    'doctorset_raw': '../data/raw/DoctorsET.json',   # Corrected relative path
    'eahci_raw': '../data/raw/EAHCI.json',
    'yetenaweg_raw': '../data/raw/yetenaweg.json'
}

# Function to read JSON file and infer schema
def infer_json_schema(file_path):
    """Infers the schema from a JSON file for SQL table creation."""
    with open(file_path, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
        if isinstance(json_data, list):  # Check if the JSON data is a list (array of objects)
            sample_data = json_data[0]  # Use the first object to infer schema
        else:
            sample_data = json_data

        column_types = {
            col: types.VARCHAR(length=max(len(str(item)) for item in sample_data[col]))  # Infer VARCHAR length
            for col in sample_data.keys()
        }
        return column_types

# Load JSON files into DataFrames and write to PostgreSQL
for table_name, file_path in json_files.items():
    # Construct the absolute path based on current working directory
    absolute_path = os.path.abspath(file_path)

    # Check if the file exists
    if not os.path.isfile(absolute_path):
        print(f"File not found: {absolute_path}")
        continue

    # Infer schema and read the JSON file
    try:
        schema = infer_json_schema(absolute_path)
        df = pd.read_json(absolute_path, dtype=str, lines=True)  # Read the JSON file into a DataFrame
        print(f"Loaded {len(df)} records from {table_name}")

        # Write the DataFrame to PostgreSQL with the inferred schema
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=schema)
        print(f"Data successfully loaded into table '{table_name}' in PostgreSQL.")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

print("Data loading process completed.")
