import pymongo

myclient = pymongo.MongoClient("mongodb://admin:123456a@localhost:27017/")

def insert_data(db, col, data):
    db_connection = myclient[db]
    collection = db_connection[col]
    return collection.insert_one(data)


def delete_data(db, col, data):
    db_connection = myclient[db]
    collection = db_connection[col]
    return collection.delete_one(data)

def delete_all(db, col):
    db_connection = myclient[db]
    collection = db_connection[col]
    return collection.delete_one({})

def find_one_data(db, col):
    db_connection = myclient[db]
    collection = db_connection[col]
    return collection.find_one({})
