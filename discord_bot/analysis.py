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

    # Variable to determine max
    _max = 0
    # Create plot
    fig = plt.figure(figsize=(10,5), dpi=100) # create fig instance
    ax = fig.add_axes([0.1,0.1,0.8,0.8])
    # Loop over dict to create bar charts. 
    for element in document.keys():
        # Create Bars 
        ax.bar(element, document[element])
        # Add text to bars
        ax.text(element, document[element] + 1, f"{document[element]}")
        # Determine Max
        max = int(document[element]) if int(document[element]) > _max else _max
    
    # Set Title and labels
    ax.set_title(f"Summary Statistics for {database} on {datetime.today().strftime('%Y-%m-%d')}")
    ax.set_ylabel("Count")

     # Set Yticks
    y_ticks = [num for num in range(0,_max + 5,5)]
    ax.set_yticks(y_ticks)

    plt.savefig(figure_path + f"summary_{database}.png")
    return figure_path + f"summary_{database}.png" # Retun location of image

def location_statistics(database:str):
    """
    Function to retrieve and visualize Location statistics
    """
    # Set up MongoDB connection. 
    client = MongoClient(MongoDB_connectionstring)
    db = client[database]
    collection = db.Analysis
    # Retrieve Summary Statistics and retrieve only fields that needed. 
    document = collection.find_one({"Title":"location statistics"}, {"_id": 0, "Title" :0})

    # Create plot
    fig = plt.figure(figsize=(10,5), dpi=100) # create fig instance
    ax = fig.add_axes([0.2,0.1,0.7,0.8])
    # Create empty list for creation of Horizontal bar
    y = []
    x = []
    # Loop over dict to create bar charts. 
    for element in document.keys():
        y.append(element)
        x.append(document[element])
    ax.barh(y, x) # Create Bar Chart
    
    # Set Title and labels
    ax.set_title(f"Location Statistics for {database} on {datetime.today().strftime('%Y-%m-%d')}")
    ax.set_ylabel("Locations")
    ax.set_xlabel("Count")

    plt.savefig(figure_path + f"location_{database}.png")
    return figure_path + f"location_{database}.png"

def skills_statistics(database:str):
    """
    Function to retrieve and visualize Skills statistics
    """
    # Set up MongoDB connection. 
    client = MongoClient(MongoDB_connectionstring)
    db = client[database]
    collection = db.Analysis
    # Retrieve Summary Statistics and retrieve only fields that needed. 
    document = collection.find_one({"Title":"skills statistics"}, {"_id": 0, "Title" :0})

    # Variable to determine max
    _max = 0
    # Create plot
    fig = plt.figure(figsize=(10,5), dpi=100) # create fig instance
    ax = fig.add_axes([0.1,0.1,0.8,0.8])

    # Loop over dict to create bar charts. 
    for element in document.keys():
        # Create Bars 
        ax.bar(element.split(" ")[0], document[element])
        # Add text to bars
        ax.text(element.split(" ")[0], document[element] + 1, f"{document[element]}")
        # Add to Max
        _max = int(document[element]) if int(document[element]) > _max else _max
    
    # Set Title and labels
    ax.set_title(f"Skills Statistics for {database} on {datetime.today().strftime('%Y-%m-%d')}")
    ax.set_ylabel("Count")
    ax.set_xlabel("Skills")

    # Set Yticks
    y_ticks = [num for num in range(0,_max + 5,5)]
    ax.set_yticks(y_ticks)

    plt.savefig(figure_path + f"skills_{database}.png")
    return figure_path + f"skills_{database}.png" # Retun location of image
