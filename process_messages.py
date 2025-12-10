import os
import datetime
import pymongo
from pymongo import MongoClient
import time
#необходима библиотека python-dotenv 1.0.1 
import pandas as pd
from pyrogram import Client

import yaml

with open('config.yaml', 'r') as stream:
    cfg = yaml.safe_load(stream)


CONFIG = {
    "telegram_api_id": cfg["telegram"]['telegram_api_id'],
    "telegram_hash": cfg["telegram"]['telegram_hash'],
}


#создание объекта-клиента Telegram 
app = Client("my_account",CONFIG["telegram_api_id"],CONFIG["telegram_hash"])



dbName=cfg["mongodb"]['db_name']
collName="msgs"
client = MongoClient(cfg["mongodb"]['url'])
db = client[dbName]
coll = db[collName]


                    

dl_history = datetime.datetime.strptime("2023-12-01", "%Y-%m-%d")
offset_date = datetime.datetime.now() - datetime.timedelta(days=30)  # Get the current date as the offset
chats = db['chats'].find({'$or': [{'updated_mg':{ '$exists':0}}, {'updated_mg':0} ]})
cc = 0
mg_cc = 0
with app:
    for chat in chats:
        try:
            if chat["Collection"] not in db.list_collection_names():
                continue
            ch_name = chat['Collection']
            for mg_id in db[ch_name].distinct('media_group_id', {}):
                msg = db[ch_name].find_one({'media_group_id': mg_id})
                mg = app.get_media_group(msg.chat.id, msg['msg_id'])
                for message in mg:
                    mg_message = db[ch_name].find_one({'msg_id': message.id})  
                    mg_message['media_group_id'] = mg_id
                    db[ch_name].replace_one({'_id':msg['_id']}, mg_message) 
                    print(mg_id)
                chat['last_mg_id'] = mg_id
                db["chats"].replace_one({'_id':chat['_id']}, chat) 
            chat['updated_mg'] = 1
            db["chats"].replace_one({'_id':chat['_id']}, chat) 
        except:
            with   open("errors.txt", "a") as file1:
                file1.write(f'{ch_name}\n')
                print(f'{ch_name} - error, sleep 5 seconds')  
                time.sleep(5)  
            continue
        last_mg_id = -1
        end_mg_id = db[chat['Collection']].find_one(sort=[('last_mg_id', 1)])['last_mg_id']
        if 'last_mg_id' in chat.keys():
            last_mg_id=chat['last_id']
        else:
            last_mg_id = db[chat['Collection']].find_one(sort=[('media_group_id', -1)])['media_group_id']
        try:
            cc = 1
            for message_original in db[chat['Collection']].find({'text':{'$in': ['', None]}, 'msg_id':{"$lte": last_id}}, sort=[('msg_id', -1)]):
                cc=0
                ccu=0
                mg = None
                mg_id = None
                for message in app.get_chat_history('@'+ch_name, offset_id=message_original['msg_id']+1, limit=1):
                    cc+=1
                    if message.id==message_original['msg_id']  and message.media_group_id:
                        text = ''
                        mg_cc +=1
                        if mg==None or mg_id != message.media_group_id:
                            mg = app.get_media_group(message.chat.id, message.id)
                            mg_id = message.media_group_id
                        for item in mg:
                            if item.caption:
                                text += item.caption  
                            if item.text:
                                text += item.text    
                        msg =  db[chat["Collection"]].find_one({'msg_id':message.id})
                        if len(text)>0:
                            if msg:
                                msg['text'] = text
                                ccu+=1  
                        msg['media_group_id'] = message.media_group_id
                        db[chat["Collection"]].replace_one({'_id':msg['_id']}, msg) 
                        print(message.id) 
                        last_id = message.id
                    chat['last_id'] = message.id-1
                    db["chats"].replace_one({'_id':chat['_id']}, chat)  
                    
                last_id = message_original['msg_id']-1
                print(f'{ch_name} - {ccu} messages saved, sleep 2 seconds')  
                time.sleep(2)   
            
            chat['updated'] = 1
            db["chats"].replace_one({'_id':chat['_id']}, chat)  

        except Exception as ex:
            with   open("errors.txt", "a") as file1:
          # append mode
                file1.write(f'{ch_name}\n')
                file1.write(f'{str(ex)}\n')
                print(str(ex))
                print(f'{ch_name} - error, sleep 5 seconds')  
                time.sleep(5)  
            continue    
    

print(f'cc = {cc}, mg_cc = {mg_cc}')
                
