import os
import json
import pandas as pd
from loguru import logger

# Set up logging
logger.add("logs/app.log", rotation="1 MB")

# Directory paths
RAW_DATA_DIR = 'data/raw'
CLEANED_DATA_DIR = 'data/cleaned'

# Create directories if they don't exist
os.makedirs(CLEANED_DATA_DIR, exist_ok=True)

# Function to flatten lists and dictionaries
def flatten_column(data):
    if isinstance(data, dict) or isinstance(data, list):
        return json.dumps(data, ensure_ascii=False)  # Ensure Amharic characters are preserved
    else:
        return data

# Function to clean data
def clean_data(file_path, channel_name):
    try:
        # Load data with UTF-8 encoding
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Flatten lists and dictionaries (using map for object columns)
        for col in df.columns:
            if df[col].dtype == 'object':  # Apply only to object columns
                df[col] = df[col].map(flatten_column)

        # Remove duplicates
        df.drop_duplicates(inplace=True)

        # Handle missing values (convert object columns to object dtype, then fill)
        object_cols = df.select_dtypes(include=['object']).columns
        df[object_cols] = df[object_cols].fillna('None')

        # Standardize formats (convert all text to lowercase)
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: x.lower() if isinstance(x, str) else x)

        # Data validation (example: ensure 'date' is a datetime)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Save cleaned data with UTF-8 encoding to preserve Amharic characters
        cleaned_file_path = os.path.join(CLEANED_DATA_DIR, f"{channel_name}_cleaned.json")
        df.to_json(cleaned_file_path, orient='records', force_ascii=False)  # Preserve Amharic characters

        logger.info(f"Cleaned data saved to {cleaned_file_path}")
    except Exception as e:
        logger.error(f"Error cleaning data from {file_path}: {e}")

def main():
    for file_name in os.listdir(RAW_DATA_DIR):
        file_path = os.path.join(RAW_DATA_DIR, file_name)
        if os.path.isfile(file_path) and file_path.endswith('.json'):
            channel_name = os.path.splitext(file_name)[0]
            clean_data(file_path, channel_name)

if __name__ == "__main__":
    main()
