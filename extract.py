# Script to scrape Indeed and store in MongoDB
from pymongo import MongoClient # For writing to MongoDB
from bs4 import BeautifulSoup
import requests
import json
import pprint
from random import randint
from time import sleep

printer = pprint.PrettyPrinter()

def create_url(jobname, location):
    jobname_without_spaces = str(jobname).replace(' ', '%20')
    return f'https://nl.indeed.com/jobs?q={jobname_without_spaces}&l={location}'

def get_all_vacancies(url):
    soup = BeautifulSoup( requests.get(url).content, "html.parser") # Reading Webpage into BeatifulSoup
    vacancies_list = soup.find_all('ul', _class = "jobsearch-ResultsList css-0")
    for vacancy in vacancies_list:
        printer.pprint(vacancy)



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


