# Supporting functions for the Discord Bot
from pymongo import MongoClient
from matplotlib import pyplot as plt
from datetime import datetime
import os

# Set variables
figure_path = os.path.dirname(__file__) + "/viz/"

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

def summary_statistics(database:str):
    """
    Function to retrieve and visualize summary statistics. 
    """
    # Set up MongoDB connection. 
    client = MongoClient(MongoDB_connectionstring)
    db = client[database]
    collection = db.Analysis
    # Retrieve Summary Statistics and retrieve only fields that needed. 
    document = collection.find_one({"Title":"summary statistics"}, {"_id": 0, "Vacancy Count":1, "Active Count": 1, "New Count":1})

    # Create plot
    fig = plt.figure(figsize=(10,5), dpi=100) # create fig instance
    ax = fig.add_axes([0.1,0.1,0.9,0.9])
    # Loop over dict to create bar charts. 
    for element in document.keys():
        ax.bar(element, document[element])
    # set text to bars. 

    # Find way to increase bar height
    
    # Set Title and labels
    ax.set_title(f"Summary Statistics for {database} on {datetime.today().strftime('%Y-%m-%d')}")
    ax.set_ylabel("Count")

    plt.savefig(figure_path + f"summary_{database}.png")

summary_statistics("Data_Engineer")
