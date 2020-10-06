import pymongo
import json 
import warnings
import pprint

DATASET_PATH = 'data/'
CONNECTION_NAME_1, CONNECTION_NAME_2 = 'weekly_demand', 'meal_info'
DATABASE_NAME = 'hotel'


def load_data(file_name, collection_name, database):
    collection = database.get_collection(collection_name)
    with open(DATASET_PATH + file_name) as f:
        file_data = json.load(f)
    collection.insert_many(file_data)
    print("Loaded {} from {}".format(collection.find().count(), file_name))


if __name__ == '__main__':
    DATABASE_PATH = "mongodb://localhost:27017/"
    client = pymongo.MongoClient(DATABASE_PATH)
    DATABASE = client[DATABASE_NAME]
    # DATABASE.create_collection(CONNECTION_NAME_1)
    # DATABASE.create_collection(CONNECTION_NAME_2)
    load_data(CONNECTION_NAME_1 + ".json", CONNECTION_NAME_1, DATABASE)
    load_data(CONNECTION_NAME_2 + ".json", CONNECTION_NAME_2, DATABASE)
    print("DATABASE {} CREATED".format(DATABASE_NAME))