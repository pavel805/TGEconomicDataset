# TGEconomicDataset
# Telegram messages downloader and processor
## Configuration
Open the config.yaml file and put mongodb connection string, telegram_api_key and hash.
## Create a channel list
channel_load.py
## Download
pyrogram_app.py
The download process will resume if it was stopped once.
## Create an additional channel list based on mantions
pyrogram_app.py
The download process will resume if it was stopped once.
## Add economic data to the messages
process_messages.py
# Collections:
1. chats - a list of Telegram channels.
2. collection with messages - exact collection name is stored in the chats collection.
