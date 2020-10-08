import pymongo
import json
import warnings
import pprint

DATASET_PATH = 'data/'
CONNECTION_NAME_1, CONNECTION_NAME_2 = 'weekly_demand', 'meal_info'
DATABASE_NAME = 'hotel'


def print_records(result, no_of_records=100):
    count = 0
    for i in result:
        if count < no_of_records:
            print(i)
        count += 1


def load_data(file_name, collection_name, database):
    collection = database.get_collection(collection_name)
    with open(DATASET_PATH + file_name) as f:
        file_data = json.load(f)
    collection.insert_many(file_data)
    print("Loaded {} from {}".format(collection.find().count(), file_name))


def show_with_query(database, collection_name, query={}, fields=None):
    collection = database.get_collection(collection_name)
    result = collection.find(query, fields)
    print_records(result, no_of_records=100)
    print("Total Records: ", result.count())


def show_with_query_agg(database, collection_name, query=[]):
    collection = database.get_collection(collection_name)
    result = collection.aggregate(query)
    print_records(result)


if __name__ == '__main__':
    DATABASE_PATH = "mongodb://localhost:27017/"
    client = pymongo.MongoClient(DATABASE_PATH)
    DATABASE = client[DATABASE_NAME]
    # DATABASE.create_collection(CONNECTION_NAME_1)
    # DATABASE.create_collection(CONNECTION_NAME_2)
    # load_data(CONNECTION_NAME_1 + ".json", CONNECTION_NAME_1, DATABASE)
    # load_data(CONNECTION_NAME_2 + ".json", CONNECTION_NAME_2, DATABASE)
    print("DATABASE {} CREATED".format(DATABASE_NAME))

    ## Query Operations
    query = {
        "$or": [{
            "center_id": {
                "$eq": 11
            }
        }, {
            "meal_id": {
                "$in": [1207, 2707]
            }
        }]
    }
    fields = {
        "center_id": 1,
        "meal_id": 1,
    }
    # show_with_query(DATABASE, CONNECTION_NAME_1, query, fields)
    query = {
        "$and": [{
            "category": {
                "$regex": "^D"
            }
        }, {
            "cuisine": {
                "$regex": "ian$"
            }
        }]
    }
    fields = {
        "_id": 0,
    }
    # show_with_query(DATABASE, CONNECTION_NAME_2, query, fields)

    ### AGGREGATION
    query = [
        ## stage 1
        {
            "$match": {
                "center_id": {
                    "$eq": 11
                }
            }
        },
        ## stage 2
        {
            "$count": "total_rows"
        }
    ]
    show_with_query_agg(DATABASE, CONNECTION_NAME_1, query)

    query = [
        ## stage 1
        {
            "$match": {
                "center_id": {
                    "$in": [11, 55]
                }
            }
        },
        ## stage 2
        {
            "$group": {
                "_id": 101,
                "avg_num_orders": {
                    "$avg": "$num_orders"
                },
                'avg_checkout_price': {
                    "$avg": '$checkout_price'
                },
                'max_checkout_price': {
                    "$max": '$checkout_price'
                },
                'total_checkout_price': {
                    "$sum": '$checkout_price'
                },
                'sample_std_dev_checkout_price': {
                    "$stdDevSamp": '$checkout_price'
                },
                'total_std_dev_checkout_price': {
                    "$stdDevPop": '$checkout_price'
                },
            }
        }
    ]
    show_with_query_agg(DATABASE, CONNECTION_NAME_1, query)