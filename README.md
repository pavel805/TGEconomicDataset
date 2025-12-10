# TGEconomicDataset
TGEconomicDataset, a new dataset containing more than 2.9 million mes-sages from the most popular Telegram channels in the field of economics in Russian, as well as a labeled mixture of these channels. The mixture is formed to test various methods of solving the problem of continuous authentication, which is of particular interest due to the need for organizations and companies to rely on data posted on social media.
# Telegram messages dataset, downloader and processor
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

# Restoring the dataset from an archive 
C:\Program Files\MongoDB\Tools\100\bin>mongorestore.exe --gzip  --verbose --archive=D:\dump\

# Collections:
1. chats - a list of Telegram channels.
2. collection with messages - exact collection name is stored in the chats collection.

# Data example
Examples of synthesized collections of types 1 and 2, presented as JSON files exported from a MongoDB database, can be viewed at the following link 
