from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
import subprocess


# Kết nối tới MongoDB (cloud)
DATABASE_MONGODB = 'retail_categories_db'
CONNECTION_STRING = "mongodb+srv://ttuan:Tuan451602%40@cluster0.e4tpme5.mongodb.net/"

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


def update_location_id_for_customer():

    db = connect_mongodb()
if __name__ == '__main__':
    update_location_id_for_customer()