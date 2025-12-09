
from pymongo import MongoClient
import pandas as pd



#with open('config.yaml', 'r') as stream:
#    cfg = yaml.safe_load(stream)
#dbName="tg8"
#collName="msgs"
#client = MongoClient(cfg["mongodb"]['url'])
#db = client[dbName]
#coll = db[collName]



uri = "mongodb://127.0.0.1:27017/" # MongoClient(cfg["mongodb"]['url']) # локальное соединение
user = "" 
pasword = ""
bd = "tg8"


# Selects collections with a similar average word count for creating synthesized datasets.
# Forms a set of collection pairs: the main collection and the one used to simulate outliers.
# The main collection is chosen, and its nearest 'neighbors' are found based on average word count.
# Then, information is generated on which two collections need to be mixed.
# The number of such infected collections is 'count'.
# inj_percent – the percentage used to estimate the minimum sufficient number of entries in the second collection.
def create_inj(count = 20, neighbours=8, min_col_length = 500, main_collection_size = 5000, inj_percent = 10 ):
    
   client = MongoClient(uri)
   db = client[bd]

   channels = db["chats"].find({})
  
   inj_c = {} # dict collection_name:average_length in words
   inj_count = {} # dict collection name:docs count
   for chl in channels:
      
     count_d = db[chl["Collection"]].count_documents({})  # document count in a collection regarderd as main
     
     #Select only collections with a number of entries not less than min_col_length
     if  count_d<min_col_length:
       continue
        
     first = chl["Collection"]
     res =  db[chl["Collection"]].aggregate([{ 
         "$group": {
         "_id": 0, #  null, 
         "avg_words": { "$avg": "$words" }
        } 
      }])
     avg_word = 0
     
     #This is one iteration of the loop, which simply records the average number of words for the given collection.
     for r in res: 
         #d_avg[chl["Collection"]] = r["avg_words"] 
         inj_c[chl["Collection"]] = r["avg_words"]
         inj_count[chl["Collection"]] = count_d
   ###
   i=0
   copy_d_avg = dict(sorted(inj_c.items(), key=lambda item: item[1]))
   c=0
   
   keys = list(copy_d_avg.keys())
   for ind, m in enumerate(copy_d_avg.keys()):
     if (c>count):
         return
     curent_key = i
     
     j=1
      
     number_nodes = inj_count[m]
     if number_nodes<main_collection_size:
        i=i+1
        continue
     # use forbiden list of collection if neccesary
     forbidden_lst = ['yaplakal', 'official_moscow', 'rhymestg', 'mosnow', 'Premiya_Darvina_18', 'sliva_net', 'tass_agency', 'almetyevskcity', 'privet_rostov_ru', 'mk_ru', 'izvestia', 'lentachold', 'rbc_news', 'koticatsme', 'Volandsellgrimace', 'finpizdec', ]

     if keys[ind] in forbidden_lst:
        i=i+1
        continue
     added = 0
     keyList = keys
     while added<neighbours and ind+j<len(keyList):
        cur_count = inj_count[keyList[ind+j]]
        if keyList[ind+j] not in forbidden_lst:
        # Add the nearest entries on the right
          if cur_count > int(main_collection_size*inj_percent/100):
              db["to_inject"].insert_one({"collection_main": keys[ind] , "collection_inj": keys[ind+j] , "avg_main":copy_d_avg[keys[ind]], "avg_inj":copy_d_avg[keys[ind+j]] ,"count_main":number_nodes, "count_inj":cur_count})
              added = added + 1
        # Add the nearest entries on the left
        if keyList[ind-j] not in forbidden_lst:
           cur_count = inj_count[keyList[ind-j]]
           if cur_count > int(main_collection_size*inj_percent/100):
             db["to_inject"].insert_one({"collection_main": keys[ind] , "collection_inj": keys[ind-j] , "avg_main":copy_d_avg[keys[ind]], "avg_inj":copy_d_avg[keys[ind-j]] ,"count_main":number_nodes, "count_inj":cur_count})
             added = added + 1
        j=j+1
     c=c+1
     i=i+1   
   #####################################   
   print("Collection to_inject created")     

   
    









# Creation of infected datasets.
# From the 'chats' collection, the names of collections used for infection are taken.
# to_inject - the collection specifying from which two collections the infected dataset is synthesized.
# injected_chats - this stores which infected datasets have been synthesized and added to MongoDB.
# size - how many entries to take from the main collection. If size=0, the entire dataset is used.
# percent - percentage of injected documents.
def make_injected_size( chat = "chats", chats_to_inject="to_inject", injected_chats ="injected_chats", min_percent = 10,   size=5000,  percent=10):
    client = MongoClient(uri)
    db = client[bd]
    if chats_to_inject not in db.list_collection_names():
       return
    
    for chats in db[chats_to_inject].find():
          
      collection_main = chats["collection_main"]
      collection_inj = chats["collection_inj"] 
      
      if collection_main not in db.list_collection_names() or collection_inj not in db.list_collection_names() :
         print(collection_main, collection_inj, "Did not find mentioned above collections")
         continue
     
      count_m =  db[collection_main].count_documents({})
      count_i = db[collection_inj].count_documents({})
      
   

       
      count_by_percent = int(count_m*percent/100) #  How many entries to take from the second collection to simulate an outlier.
      if (size>0):
          if (count_m < size or count_i< size*percent):
             continue # Collections are skipped if they do not have a sufficient volume.
          count_m = int(size*(1-percent/100))
          count_i = int(round(size*percent/100))
      else:       
        # If there are not enough outlier entries for injection, the available entries are used and the percentage is adjusted.
        if count_by_percent > count_i:
          count_by_percent = count_i
          percent = int(100*count_i/(count_m+count_i))
          
          
        if percent<min_percent:
             continue
         
      result_collection = collection_main+"_and_"+collection_inj +"_" + str(percent) + "_size_" + str(count_m+count_i)  + "_type1"

    
      posts_m = db[collection_main].find().limit(count_m).sort("timestamp", +1)
      posts_i = db[collection_inj].find().limit(count_by_percent).sort('timestamp', -1)
       
      avg_d = 0
      d1 = 0
      d2 = 0
      i=0
      for m in posts_m:
         i=i+1
         d1 = m["timestamp"]
         if (d2 != 0):
           avg_d = avg_d + d2-d1
         d2=d1  
      
      avg_d = avg_d/i # The average number of seconds between messages
      days = int(avg_d/(60*60*24))
      
   
       # if these collections already exist, they are deleted and recreated.-- 
      if (result_collection in db.list_collection_names() ):
         db[result_collection].delete_many({})
         db[injected_chats].delete_many({"collection":result_collection})
      
      
      max_date = "0"
      max_id = -1 
      from datetime import datetime  
      # First, non outlier entries are added.

      posts_m = db[collection_main].find().sort("timestamp", +1).limit(count_m)
      for post in posts_m:
        inserted_id = db[result_collection].insert_one(post)
        db[result_collection].update_one({"_id": inserted_id.inserted_id }, { "$set": { "outlier": 0, "new_timestamp":post['timestamp'] } }) 
        #inserted_id =  db[result_collection].insert_one({"message_id": post["message_id"], "message": post["message"], "date": post["date"], "outlier": 0 })
        
        # The most recent date for which entries exist is examined.
        if datetime.fromtimestamp(int(max_date)) < datetime.fromtimestamp(int(post["timestamp"])):
           max_date = post["timestamp"]
        
      i=1
      # Entries from the second dataset are now added, with the date synthesized based on the average date.
      post_i1 = db[collection_inj].find().sort('timestamp', -1).limit(count_by_percent)
       
      post_i = [document for document in post_i1]
      post_i = post_i.reverse()
      
     
      for post in posts_i:
        inserted_id = db[result_collection].insert_one(post)
        db[result_collection].update_one({"_id": inserted_id.inserted_id }, { "$set": { "outlier": 1, "new_timestamp":str(int(max_date)+avg_d) } }) 
        #inserted_id =  db[result_collection].insert_one({"message_id": str(max_id+i), "message": post["message"], "date": str(int(max_date)+i*60*60*24), "outlier": 1} )
        i = i + 1
      #  ---
      db[injected_chats].insert_one({"collection":result_collection, "title":result_collection, "Source":"inj", "type": "type1", "collection_main":collection_main, "collection_inj":collection_inj, "percent":percent, "avg_word_main": chats['avg_main'] , "avg_word_inj": chats["avg_inj"], "main_count": count_m, "injected_count":count_i , "size":size})
####################################################################################


#############################################################################################3
def remove_all_injected( chats_to_inject="to_inject", injected_chats ="injected_chats"):
    client = MongoClient(uri)
    db = client[bd]
    
    for chats in db[injected_chats].find(): 
      collection = chats["title"]
      db[collection].delete_many({})
      db[collection].drop()
    db[injected_chats].delete_many({})
    #db[chats_to_inject].delete_many({})  
##########################################################################3 
    


# Idea of synthesis: select entries from the second collection, which simulates outliers, and check the start and end dates.
# Remove entries from the main first collection within this date range and insert data from the second collection as outliers.
# chat - information about the collections being used
# chats_to_inject - source data about pairs of collections to be mixed
# injected_chats - collection where the result is recorded, indicating that the artificial collection has been synthesized
# percent_inj - percentage of documents marked as outliers (may vary if Telegram channels differ significantly in posting frequency)
 
def make_injected_datacoinside(chat = "chats", chats_to_inject="to_inject", injected_chats ="injected_chats",   percent_inj=10):
    min_percent = 8
    max_percent = 15
 
 
    client = MongoClient(uri)
    db = client[bd]
    if chats_to_inject not in db.list_collection_names():
       return
   
    for chats in db[chats_to_inject].find(): 
      
      percent=percent_inj
      id_inj = chats["_id"]
      collection_main = chats["collection_main"]
      collection_inj = chats["collection_inj"] 

      #result_collection = collection_main+"_and_"+collection_inj +"_" + str(percent) + "_type2"
     
      count_m =  db[collection_main].count_documents({})
      count_i = db[collection_inj].count_documents({})
      count_by_percent = int(count_m*percent/100) #  How many entries to take from the second collection to simulate an outlier.
          
    # If there are not enough entries for injection, use the available entries and adjust the percentage accordingly.
      if count_by_percent > count_i:
        count_by_percent = count_i
        percent = int(100*count_i/(count_m+count_by_percent))
    


    # 
      
      posts_i = db[collection_inj].find().sort('timestamp', -1).limit(count_by_percent)
      
      # Find the minimum and maximum dates of the injecting posts.
      posts_i1 = list(db[collection_inj].find().sort('timestamp', -1).limit(count_by_percent)) 
      
      max_date_m = posts_i1[0]["timestamp"]
      min_date_m = posts_i1[len(posts_i1)-1]["timestamp"]
      
      count_deleted = db[collection_main].count_documents({"$and": [{"timestamp": {"$gt":min_date_m}},{"timestamp": {"$lt":max_date_m}} ]})

      percent1 = int(round(100*count_by_percent/(count_m+count_by_percent-count_deleted)))
      
      if percent1 > max_percent or percent1 < min_percent:
        continue
      
      result_collection = collection_main+"_and_"+collection_inj +"_" + str(percent1) +  "_type2"
      
      
      # Next, posts within these dates should not be included in the final collection.
      # The final percentage of added entries may be different, as the posting frequency
      # can vary in both channels.

      # If these collections already exist, they are deleted and recreated.
      if (result_collection in db.list_collection_names() ):
         db[result_collection].delete_many({})

      max_date = "0"
      max_id = -1 
      from datetime import datetime  
   
      posts_m = db[collection_main].find().sort('timestamp', 1) # в порядке возрастания timestamp это значит от ранних дат к наиболее свежим
      for post in posts_m:
        # 
        if post['timestamp'] <=max_date_m and post['timestamp']>=min_date_m :
           # 
           continue
        inserted_id = db[result_collection].insert_one(post)
        db[result_collection].update_one({"_id": inserted_id.inserted_id }, { "$set": { "outlier": 0, "new_timestamp":post['timestamp'] } }) 
    
        
    
        
      # IDs for the injecting entries should continue sequentially, and the dates should correspond accordingly.
      i=0
      posts_i_added = db[collection_inj].find().sort('timestamp', -1).limit(count_by_percent)
      post_i_added = ([document for document in posts_i_added])#.reverse()
      post_i_added.reverse()
      for post1 in post_i_added:
        inserted_id = db[result_collection].insert_one(post1)
        db[result_collection].update_one({"_id": inserted_id.inserted_id }, { "$set": { "outlier": 1, "new_timestamp":post1['timestamp'] } }) 
       

        i = i + 1
      # Record that the collection has been added ---
      
      db[injected_chats].insert_one({"collection":result_collection, "title":result_collection, "Source":"inj", "type": "type2", "collection_main":collection_main, "collection_inj":collection_inj, "percent":percent1,  "main_count": count_m-count_deleted, "injected_count":count_by_percent , "size":count_m+count_by_percent-count_deleted, "avg_word_main": chats['avg_main'] , "avg_word_inj": chats["avg_inj"]})
    

 




create_inj(count = 20, neighbours=10)
make_injected_datacoinside()
make_injected_size(size=5000)
#remove_all_injected()
