from pyrogram import Client
from dotenv import load_dotenv
import os
from pyrogram.raw import functions, types
import datetime
import pymongo
from pymongo import MongoClient
import time
#необходима библиотека python-dotenv 1.0.1 
import pandas as pd
import yaml

with open('config.yaml', 'r') as stream:
    cfg = yaml.safe_load(stream)


df_dxy = pd.read_csv("DXY.csv")
df_gd = pd.read_csv("GD.csv")
df_btc = pd.read_csv("BTC.csv")
df_rub = pd.read_csv("RUB.csv")
df_brent = pd.read_csv("Brent.csv")
dbName="tg7"
collName="msgs"
client = MongoClient(cfg["mongodb"]['url'])
db = client[dbName]
coll = db[collName]

load_dotenv()

CONFIG = {
    "telegram_api_id": cfg["telegram"]['telegram_api_id'],
    "telegram_hash": cfg["telegram"]['telegram_hash'],
}

def message_to_dictionary(message):
    msg_doc = {'text': message.text, 'timestamp': datetime.datetime.timestamp(message.date) }
    msg_doc ['posted_str_date']= message.date.strftime("%m/%d/%Y") 
    msg_doc['forward_from_user_username'] = None
    msg_doc['forward_from_chat_username'] = None
    if message.forward_from:
        if message.forward_from.username:
            msg_doc['forward_from_user_username'] = '@'+message.forward_from.username     
        msg_doc['forward_from_user_uid'] = str(message.forward_from.id)
    if message.forward_sender_name:
        msg_doc['forward_sender_name'] = '@'+message.forward_sender_name 
    if message.forward_from_chat:
        if message.forward_from_chat.username:
            msg_doc['forward_from_chat_username'] = '@'+message.forward_from_chat.username
        msg_doc['forward_from_chat_uid'] = str(message.forward_from_chat.id)
    
    if message.forward_from_message_id:
        msg_doc['forward_from_message_id'] = message.forward_from_message_id 
    if message.forward_date:
        msg_doc['forward_date'] = message.forward_date
   
    dxy_row =  df_dxy[df_dxy['Date']==msg_doc ['posted_str_date']].iloc[0]
    gd_row =  df_gd[df_gd['Date']==msg_doc ['posted_str_date']].iloc[0]
    btc_row =  df_btc[df_btc['Date']==msg_doc ['posted_str_date']].iloc[0]
    rub_row =  df_rub[df_rub['Date']==msg_doc ['posted_str_date']].iloc[0]
    brent_row =  df_brent[df_brent['Date']==msg_doc ['posted_str_date']].iloc[0]


    #,,,
    msg_doc['timestamp'] = message.date.timestamp()
    msg_doc['posted'] =  message.date
    msg_doc['market_entities'] = {}
    msg_doc['market_entities']['dxy'] = {"name": 'dxy', 'description':'usd index','Open': float(dxy_row['Open']), 'Close': float(dxy_row['Close']), 'Low': float(dxy_row['Low']),'High': float(dxy_row['High']),'200MA': float(dxy_row['200MA']),'AboveMA': float(dxy_row['AboveMA']),'AboveMADays': float(dxy_row['AboveMADays']), 'RSI':float(dxy_row['RSI'])}
    msg_doc['market_entities']['gold'] = {"name": 'gold', 'description':'gold infinite future price', 'Open': float(gd_row['Open']),'Close': float(gd_row['Close']),'Low': float(gd_row['Low']),'High': float(gd_row['High']),'200MA': float(gd_row['200MA']),'AboveMA': float(gd_row['AboveMA']),'AboveMADays': float(gd_row['AboveMADays']), 'RSI':float(gd_row['RSI'])}
    msg_doc['market_entities']['btc'] = {"name": 'btc', 'description':'bitcoin price', 'Open': float(btc_row['Open']),'Close': float(btc_row['Close']),'Low': float(btc_row['Low']),'High': float(btc_row['High']),'200MA': float(btc_row['200MA']),'AboveMA': float(btc_row['AboveMA']),'AboveMADays': float(btc_row['AboveMADays']), 'RSI':float(btc_row['RSI'])}

    msg_doc['market_entities']['RUB'] = {"name": 'btc', 'description':'ruble quote', 'Open': float(rub_row['Open']),'Close': float(rub_row['Close']),'Low': float(rub_row['Low']),'High': float(rub_row['High']),'200MA': float(rub_row['200MA']),'AboveMA': float(rub_row['AboveMA']),'AboveMADays': float(rub_row['AboveMADays']), 'RSI':float(rub_row['RSI'])}
    msg_doc['market_entities']['BR'] = {"name": 'btc', 'description':'brent oil price', 'Open': float(brent_row['Open']),'Close': float(brent_row['Close']),'Low': float(brent_row['Low']),'High': float(brent_row['High']),'200MA': float(brent_row['200MA']),'AboveMA': float(brent_row['AboveMA']),'AboveMADays': float(brent_row['AboveMADays']), 'RSI':float(brent_row['RSI'])}
      
    msg_doc["views"] = message.views
    msg_doc["msg_id"] = message.id
    msg_doc["forwards"] = message.forwards
    msg_doc["outgoing"] = message.outgoing
    msg_doc["media"] = str(message.media)
    reactions = []
    if message.reactions:
        for reaction in message.reactions.reactions:
            react = {"emoji": reaction.emoji, "count": reaction.count}
            reactions.append(react)
    msg_doc["reactions"] = reactions
    msg_doc["edit_date"] = message.edit_date
    msg_doc["mentioned"] = message.mentioned
    msg_doc["scheduled"] = message.scheduled
    msg_doc["from_scheduled"] = message.from_scheduled
   
    if message.text:
        msg_doc['text'] = message.text
        msg_entities = message.entities
    else:
        msg_doc['text'] = message.caption
        msg_entities = message.caption_entities  
    hashtags = []
    tg_urls = []
    entities = []
    mentions = []
    if msg_entities:
        for entity in msg_entities:
            #custom_emoji_id
            if str(entity.type) == "MessageEntityType.HASHTAG":
                hashtag = msg_doc['text'][entity.offset:entity.offset + entity.length]
                hashtags.append(hashtag)
            if str(entity.type) == "MessageEntityType.MENTION":
                mention = msg_doc['text'][entity.offset:entity.offset + entity.length]
                mentions.append(mention)
            if str(entity.type) in ["MessageEntityType.TEXT_LINK", "MessageEntityType.URL"]:
                tg_url = msg_doc['text'][entity.offset:entity.offset + entity.length]
                if tg_url.startswith("https://t.me/"):        
                    tg_urls.append(tg_url)
                entities.append({"type": str(entity.type), "offset": entity.offset, "length": entity.length})
            
            else:
                entities.append({"type": str(entity.type), "offset": entity.offset, "length": entity.length})
            
        # if entity.type == "MessageEntityType.MENTION":
        #     tg_url = msg_doc['text'][entity.offset:entity.offset + entity.length]
        #     if tg_url.starts("https://t.me/"):        
        #         tg_urls.append(entity.url)
        # if entity.type == "MessageEntityType.MENTION":
        #     tg_url = msg_doc['text'][entity.offset:entity.offset + entity.length]
        #     if tg_url.starts("https://t.me/"):        
        #         tg_urls.append(entity.url)
        #if entity.type == "MessageEntityType.TEXT_MENTION":
    msg_doc["hashtags"]  = hashtags
    msg_doc["entities"]  = entities
    msg_doc["mentions"]  = mentions
    if None in tg_urls:
        print('null')
    msg_doc["tg_urls"]  = tg_urls
    #caption_entities
    return msg_doc
                    

#создание объекта-клиента Telegram 
app = Client("my_account",CONFIG["telegram_api_id"],CONFIG["telegram_hash"])

#список сообщений для выгрузки
messages = []

dl_history = datetime.datetime.strptime("2022-09-01", "%Y-%m-%d")
offset_date = datetime.datetime.strptime("2024-12-01", "%Y-%m-%d")# datetime.datetime.now() - datetime.timedelta(days=30)  # Get the current date as the offset


with app:
    #app.send_message("me", "Hello from pyrogram")
    #получение списка диалогов

    with open('links2.txt', 'r') as file:
    # Read each line in the file
        for ch_name in file:
            coll_name = ch_name[1:].rstrip()
            coll = db[coll_name]
            chat_query={"Title":coll_name}
            chat = db["chats"].find_one(chat_query)
            ch_mentions = set()
            tg_urls = set()
            forwards_from_chat = set()
            forwards_from_user = set()
            chat_msg_number = 0
            last_msg_id = 0
            date_stop = False
            if chat is None:
                try:
                    tg_chat = app.get_chat(ch_name)
                    #channel =  app.resolve_peer(tg_chat.id)
                    #r =  app.invoke(functions.channels.GetFullChannel(channel=channel ))
                    members_count = tg_chat.members_count
                   
                    # if app.search_messages_count(ch_name, "золото")==0:
                    #     print(f'{ch_name} no search results')
                    #     continue
                    chat = {"Title":coll_name, "posted": datetime.datetime.now(), "timestamp": datetime.datetime.now().timestamp(), "Collection": coll_name, "Source": "tg", "ch_mentions":[],"forwards_from_chat":[], "forwards_from_user":[], "tg_urls":[], "Origin": "tg_stat"}
                    
                    chat["members_count"]  =  members_count
                    print(f'{ch_name} saved to the chats collection, sleep 7 seconds')   
                    db["chats"].insert_one(chat)
                    time.sleep(7)   
                except Exception as ex:
                    print(ex)
                    time.sleep(3)  
                    continue
            else:
                ch_mentions= set(["ch_mentions"])
                tg_urls = set(chat["tg_urls"])
                forwards_from_chat = set(chat["forwards_from_chat"])
                forwards_from_user = set(chat["forwards_from_user"])

                last_msg_id = 0
                chat_msg_number = db[chat["Collection"]].count_documents({})
                if chat_msg_number > 0:
                    message = db[chat["Collection"]].find_one({}, sort=[('msg_id', pymongo.ASCENDING)])
                    last_msg_id = message["msg_id"]
                    #check for the case when the last message items were not added to the chat attributes
                    if message['forward_from_chat_username']:
                        for item in message['forward_from_chat_username']:
                            if item not in chat['forwards_from_chat']:
                                chat['forwards_from_chat'].append(item)
                    if message['forward_from_user_username']:
                        for item in message['forward_from_user_username']:
                            if item not in chat['forwards_from_user']:
                                chat['forwards_from_user'].append(item)
                    if message['mentions']:
                        for item in message['mentions']:
                            if item not in chat['ch_mentions']:
                                chat['ch_mentions'].append(item)
                    if message['tg_urls']:
                        for item in message['tg_urls']:
                            if item not in chat['tg_urls']:
                                chat['tg_urls'].append(item)
                    if message['posted'].timestamp() < dl_history.timestamp():
                        date_stop = True
            try:
                #dl_history = datetime.strptime("2024-02-17", "%Y-%m-%d")
               
                while not date_stop:
                    if last_msg_id >0:
                        messages = app.get_chat_history(ch_name, offset_id=last_msg_id, limit=500)
                    else:
                        messages = app.get_chat_history(ch_name, offset_date=offset_date, limit=500)
                    msg_c = 0
                    for message in messages:
                        if message.date.timestamp() < dl_history.timestamp():
                            date_stop = True
                            break
                        # Break the loop if message date is older than dl_history
                        print(message)
                        message_dictionary = message_to_dictionary(message)
                        coll.insert_one(message_dictionary)
                        msg_c += 1
                        print(message_dictionary)
                        ch_mentions =  ch_mentions.union(set(message_dictionary["mentions"]))  
                        tg_urls = tg_urls.union(set(message_dictionary["tg_urls"])) 
                        
                        if message_dictionary['forward_from_chat_username']:
                            forwards_from_chat.add(message_dictionary['forward_from_chat_username'])
                        elif message_dictionary['forward_from_user_username']:
                            forwards_from_user.add(message_dictionary['forward_from_user_username'])

                        last_msg_id = message_dictionary["msg_id"]
                        chat["ch_mentions"]  = list(ch_mentions)
                        chat["tg_urls"] = list(tg_urls)
                        chat["forwards_from_chat"]  = list(forwards_from_chat)
                        chat["forwards_from_user"]  = list(forwards_from_user)
                        db["chats"].replace_one({'_id':chat['_id']}, chat)   
                    if msg_c == 0:
                        date_stop = True    
                    print(f'{ch_name} - {msg_c} messages saved, sleep 5 seconds')  
                    time.sleep(5)    
 
               
            except Exception as ex:
                print(ex)
                pass


   

 
