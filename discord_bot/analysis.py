# Supporting functions for the Discord Bot
from pymongo import MongoClient

MongoDB_connectionstring = "mongodb://localhost:27017"

def get_new(database:str) -> list:
    """
    Functions that get all new vacancies. 
    Return list with dicts
    """
    client = MongoClient(MongoDB_connectionstring)
    db = client[database]
    collection = db.DataStore

    result_list = []

    for document in collection.find({"New":True}, {"_id":0, "Job_title":1, "Organization":1, "Location":1, "URL":1}):
        result_list.append(document)
    
    return result_list

# for vacancy in get_new("Product_Owner"):
#     print(f"Job: {vacancy['Job_title']}")