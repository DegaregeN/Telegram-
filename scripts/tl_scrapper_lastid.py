import logging
from telethon import TelegramClient
import csv
import os
import json
from dotenv import load_dotenv
# Set up logging
logging.basicConfig(
    filename='scraping.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables once
load_dotenv('.env')
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')
phone = os.getenv('phone')

# Function to get last processed message ID
def get_last_processed_id(channel_username):
    try:
        with open(f"{channel_username}_last_id.json", 'r') as f:
            return json.load(f).get('last_id', 0)
    except FileNotFoundError:
        logging.warning(f"No last ID file found for {channel_username}. Starting from 0.")
        return 0

# Function to save last processed message ID
def save_last_processed_id(channel_username, last_id):
    with open(f"{channel_username}_last_id.json", 'w') as f:
        json.dump({'last_id': last_id}, f)
        logging.info(f"Saved last processed ID {last_id} for {channel_username}.")

# Function to scrape data from a single channel
async def scrape_channel(client, channel_username, writer, media_dir):
    try:
        entity = await client.get_entity(channel_username)
        channel_title = entity.title
        
        last_id = get_last_processed_id(channel_username)
        
        # Limit to scraping only 20 messages (changed from 10 to 20)
        message_count = 0
        async for message in client.iter_messages(entity):
            if message.id <= last_id:
                continue
            
            media_path = None
            if message.media:
                filename = f"{channel_username}_{message.id}.{message.media.document.mime_type.split('/')[-1]}" if hasattr(message.media, 'document') else f"{channel_username}_{message.id}.jpg"
                media_path = os.path.join(media_dir, filename)
                await client.download_media(message.media, media_path)
                logging.info(f"Downloaded media for message ID {message.id}.")
            
            writer.writerow([channel_title, channel_username, message.id, message.message, message.date, media_path])
            logging.info(f"Processed message ID {message.id} from {channel_username}.")
            
            last_id = message.id
            message_count += 1
            
            # Stop after scraping 20 messages
            if message_count >= 100:
                break

        save_last_processed_id(channel_username, last_id)

        if message_count == 0:
            logging.info(f"No new messages found for {channel_username}.")

    except Exception as e:
        logging.error(f"Error while scraping {channel_username}: {e}")

# Initialize the client once with a session file
client = TelegramClient('scraping_session', api_id, api_hash)

async def main():
    try:
        await client.start(phone)
        logging.info("Client started successfully.")
        
        media_dir = 'photos'
        os.makedirs(media_dir, exist_ok=True)

        with open('scraped_data.csv', 'a', newline='', encoding='utf-8') as file:  # Changed file name
            writer = csv.writer(file)
            writer.writerow(['Channel Title', 'Channel Username', 'ID', 'Message', 'Date', 'Media Path'])
            
            channels = [
                "https://t.me/DoctorsET",  
                "https://t.me/CheMed123",
                "https://t.me/lobelia4cosmetics",
                "https://t.me/yetenaweg",
                "https://t.me/EAHCI"
                ]
            
            for channel in channels:
                await scrape_channel(client, channel, writer, media_dir)
                logging.info(f"Scraped data from {channel}.")

    except Exception as e:
        logging.error(f"Error in main function: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
