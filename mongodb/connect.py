from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
import subprocess
# Kết nối tới MongoDB (cloud)
import os
DATABASE_MONGODB = 'retail_categories_db'
CONNECTION_STRING = "your connection string to mongodb"

filename = '.secretkey'

if os.path.isfile(filename):
    with open(filename, 'r') as f:
        first_line = f.readline().strip()
        CONNECTION_STRING = first_line

# print(f"CONNECTION_STRING: {CONNECTION_STRING}")

def connect_mongodb(database = DATABASE_MONGODB):
    client = MongoClient(CONNECTION_STRING)
    db = client[database]
    return db

# get collection in dataframe
def get_collection(collection_name):
    conn = connect_mongodb()
    collection = conn[collection_name]
    data = list(collection.find())
    df = pd.DataFrame(data)

    return df

def update_location_id_for_customer():

    db = connect_mongodb()
    # 1. Aggregate + $lookup để join
    pipeline = [
        {
            '$lookup': {
                'from': 'location',
                'let': {'customer_ward': '$ward', 'customer_city': '$city'},
                'pipeline': [
                    {'$match': {
                        '$expr': {
                            '$and': [
                                {'$eq': ['$name', '$$customer_ward']},
                                {'$eq': ['$city_name', '$$customer_city']}
                            ]
                        }
                    }}
                ],
                'as': 'loc'
            }
        },
        {
            '$addFields': {
                'ward_id': {'$arrayElemAt': ['$loc._id', 0]}
            }
        },
        {'$project': {'loc': 0}}
    ]

    result = list(db.customer.aggregate(pipeline))

    print(result[1:10])

    for doc in result:
        if 'ward_id' in doc and doc['ward_id']:
            db.customer.update_one(
                {'_id': doc['_id']},
                {'$set': {'ward_id': doc['ward_id']}}
            )
    return


# def update_location_id_for_customer():
#
#     db = connect_mongodb()
if __name__ == '__main__':
    print(get_collection('product'))