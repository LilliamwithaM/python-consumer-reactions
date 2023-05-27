# Import some necessary modules
# pip install kafka-python
# pip install pymongo
# pip install "pymongo[srv]"
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi

import json

uri = "mongodb+srv://jona2708:Jonathan2708@bdnosql.p8tt50o.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
#client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection

#try:
#    client.admin.command('ping')
#    print("Pinged your deployment. You successfully connected to MongoDB!")
#except Exception as e:
#    print(e)

# Connect to MongoDB and pizza_data database

try:
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.bdnosql
    print("MongoDB Connected successfully!")
except:
    print("Could not connect to MongoDB")


consumer = KafkaConsumer('reactions',bootstrap_servers=['my-kafka-0.my-kafka-headless.jona27081.svc.cluster.local:9092'])
# Parse received data from Kafka
for msg in consumer:
    record = json.loads(msg.value)
    print(record)
    reactionName = record["reactionName"]

    # Create dictionary and ingest data into MongoDB
    try:
       reaction_rec = {'reactionName':reactionName }
       print (reaction_rec)
       reaction_id = db.bdnosql_info.insert_one(reaction_rec)
       print("Data inserted with record ids", reaction_id)
    except Exception as e:
        print("Could not insert into MongoDB:", e)

    #Create bdnosql_sumary and insert groups into mongodb
    try:
        agg_result = db.bdnosql_info.aggregate(
            [{
                "$group" :
                { "_id" : "$reactionName",
                  "n" : {"$sum": 1}
                }}
            ])
        db.bdnosql_sumary.delete_many({})
        for i in agg_result:
            print(i)
            sumary_id = db.bdnosql_sumary.insert_one(i)
            print("Sumary inserted with record ids: " ,sumary_id)
    except Exception as e:
        print(f'group vy cought {type(e)}: ')
        print(e)



