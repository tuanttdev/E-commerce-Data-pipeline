from pymongo import MongoClient
import pandas as pd
from sqlalchemy import create_engine
import subprocess
from dotenv import load_dotenv
import os
import random
from datetime import datetime, timedelta

from sqlalchemy.util import timezone

load_dotenv()  # nạp biến môi trường từ file .env
DATABASE_MONGODB = 'retail_categories_db'
CONNECTION_STRING = os.getenv('MONGODB_CONNECTION_STRING')

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
#
# def update_location_id_for_customer():
#
#     db = connect_mongodb()
#     # 1. Aggregate + $lookup để join
#     pipeline = [
#         {
#             '$lookup': {
#                 'from': 'location',
#                 'let': {'customer_ward': '$ward', 'customer_city': '$city'},
#                 'pipeline': [
#                     {'$match': {
#                         '$expr': {
#                             '$and': [
#                                 {'$eq': ['$name', '$$customer_ward']},
#                                 {'$eq': ['$city_name', '$$customer_city']}
#                             ]
#                         }
#                     }}
#                 ],
#                 'as': 'loc'
#             }
#         },
#         {
#             '$addFields': {
#                 'ward_id': {'$arrayElemAt': ['$loc._id', 0]}
#             }
#         },
#         {'$project': {'loc': 0}}
#     ]
#
#     result = list(db.customer.aggregate(pipeline))
#
#     print(result[1:10])
#
#     for doc in result:
#         if 'ward_id' in doc and doc['ward_id']:
#             db.customer.update_one(
#                 {'_id': doc['_id']},
#                 {'$set': {'ward_id': doc['ward_id']}}
#             )
#     return
#


def random_datetime_last_2_years():
    now = datetime.utcnow()
    # 2 năm = khoảng 730 ngày
    days_in_2_years = 730

    # Random số ngày cách đây từ 0 đến 730 ngày
    random_days_ago = random.randint(0, days_in_2_years)

    # Random thêm số giây trong ngày (0 đến 86399)
    random_seconds = random.randint(0, 86399)

    random_date = now - timedelta(days=random_days_ago, seconds=random_seconds)
    return random_date

def update_create_update_time_for_product():
    db = connect_mongodb()
    collection = db.city  # hoặc db.product nếu collection tên 'product'

    # Lấy toàn bộ document
    documents = list(collection.find())
    print(documents[1])
    print(len(documents))
    now = datetime(year=2025,month=7,day=1)

    for doc in documents:
        # now = random_datetime_last_2_years()
        print(now)
        # Tạo dữ liệu update
        update_fields = {
            "createdAt": now,
            "update_date": now
        }

        # Update document theo _id
        collection.update_one({"_id": doc["_id"]}, {"$set": update_fields})

    print(f"Đã cập nhật {len(documents)} document")

    return


if __name__ == '__main__':
    update_create_update_time_for_product()