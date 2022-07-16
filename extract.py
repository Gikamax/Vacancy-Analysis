# Script to scrape Indeed and store in MongoDB
from pymongo import MongoClient # For writing to MongoDB
from bs4 import BeautifulSoup
import requests
import json
import pprint
from random import randint
from time import sleep

printer = pprint.PrettyPrinter()

def create_url(jobname:str, location:str) -> str:
    """
    Function to generate indeed Url based on Jobname and Location. 
    Accepts Jobname with spaces
    """
    jobname_without_spaces = str(jobname).replace(' ', '%20')
    return f'https://nl.indeed.com/jobs?q={jobname_without_spaces}&l={location}'

def get_all_vacancies(url:str) -> list:
    """
    Based on valid Indeed url grabs all sub-urls for vacancies on the page. 
    Loops through Indeed and to prevent bot detection rand sleep are added. 
    """
    vacancies_url = [] # Empty list to append sub-urls. 
    soup = BeautifulSoup( requests.get(url).content, "html.parser") # Reading Webpage into BeatifulSoup
    jobcards = soup.find('div', id = 'mosaic-provider-jobcards') # Locating part of webpage with only vacancies. 
    jobcards_headers = jobcards.find_all('h2') # find all Jobheaders (Titles)
    # Iterate over every Title to grab the url. 
    for header in jobcards_headers:
        url = "https://nl.indeed.com" + header.find('a')['href'] # Add base url to href output. 
        vacancies_url.append(url) # add found url to list
    return vacancies_url # return list. 



get_all_vacancies(create_url('Timmerman', 'Enschede'))



# ##### MongoDB connection and insertions
# # Create MongoDB client 
# client = MongoClient("mongodb://localhost:27017")

# # Create test database
# test_db = client['test']

# def insert_test_doc():
#     collection = test_db.test
#     test_document = {
#         "name": "Gikamax",
#         "It works": 0
#     }
#     collection.insert_one(test_document)
#     print("Succes")

# insert_test_doc()


