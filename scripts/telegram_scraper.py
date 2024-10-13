import os
import json
from telethon.sync import TelegramClient
from telethon.tl.types import InputMessagesFilterPhotos
from datetime import datetime, timezone
from loguru import logger
from dotenv import load_dotenv
import asyncio

# Load environment variables from .env file
load_dotenv()

# Configuration
api_id = os.getenv('API_ID')
api_hash = os.getenv('API_HASH')
phone = os.getenv('PHONE')

# Define the channels for data collection and image scraping
data_channels = [
    'DoctorsET',
    'EAHCI',
    'yetenaweg'
]

image_channels = [
    'CheMed123',
    'lobelia4cosmetics'
]

# Directory to save collected data and images
DATA_DIR = 'data/raw'
IMAGE_DIR = os.path.join(DATA_DIR, 'telegram_images')

# Create directories if they don't exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(IMAGE_DIR, exist_ok=True)

# Set up logging
logger.add("logs/app.log", rotation="1 MB")

# Connect to Telegram client
client = TelegramClient('session_name', api_id, api_hash)

# Helper function to make data JSON-serializable
def make_json_serializable(data):
    if isinstance(data, bytes):
        return data.decode('utf-8', errors='ignore')
    elif isinstance(data, datetime):
        return data.isoformat()
    elif isinstance(data, dict):
        return {k: make_json_serializable(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [make_json_serializable(v) for v in data]
    else:
        return data

# Function to collect data from a Telegram channel
async def collect_data(channel):
    try:
        messages = await client.get_messages(channel, limit=1000)  # Set an appropriate limit
        data = []
        for message in messages:
            msg_dict = message.to_dict()
            msg_dict = make_json_serializable(msg_dict)
            data.append(msg_dict)
        logger.info(f"Collected {len(messages)} messages from {channel}")
        return data
    except Exception as e:
        logger.error(f"Error collecting data from {channel}: {e}")
        return []

# Function to save data as JSON
def save_data(data, path):
    try:
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, default=str, ensure_ascii=False)
        logger.info(f"Saved data to {path}")
    except Exception as e:
        logger.error(f"Error saving data to {path}: {e}")

# Function to download images from a Telegram channel
async def download_images(channel, start_date=None, end_date=None):
    channel_image_dir = os.path.join(IMAGE_DIR, channel)
    os.makedirs(channel_image_dir, exist_ok=True)
    try:
        async for message in client.iter_messages(channel, filter=InputMessagesFilterPhotos):
            message_date = message.date.replace(tzinfo=timezone.utc)  # Make message date timezone aware
            if (start_date and message_date < start_date) or (end_date and message_date > end_date):
                continue
            # Download the photo
            await client.download_media(message.photo, file=os.path.join(channel_image_dir, f'{message.id}.jpg'))
        logger.info(f"Downloaded images from {channel}")
    except Exception as e:
        logger.error(f"Error downloading images from {channel}: {e}")

# Main function to collect data and download images
def main():
    with client:
        # Data collection for each channel
        for channel in data_channels:
            logger.info(f"Collecting data from {channel}")
            data = client.loop.run_until_complete(collect_data(channel))
            if data:
                data_path = os.path.join(DATA_DIR, f"{channel}.json")
                save_data(data, data_path)
            else:
                logger.warning(f"No data collected from {channel}")

        # Image scraping for each channel
        for channel in image_channels:
            logger.info(f"Downloading images from {channel}")
            client.loop.run_until_complete(download_images(
                channel, 
                start_date=datetime(2022, 5, 1, tzinfo=timezone.utc), 
                end_date=datetime(2024, 10, 10, tzinfo=timezone.utc)
            ))

if __name__ == "__main__":
    main()
