# Script to scrape Indeed and store in MongoDB
from pymongo import MongoClient # For writing to MongoDB
from bs4 import BeautifulSoup
import requests
import json
import pprint
from random import randint
from time import sleep
from datetime import datetime

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

def write_vacancy_details(vacancies_url, MongoDB_connectionstring:str=''):
    """
    Grabs all vacancy details and writes them to MongoDB
    """ 
    # for vacancy in vacancies_url:
    #     pass
    vacancy = vacancies_url
    soup = BeautifulSoup( requests.get(vacancy).content, "html.parser")
    # Title
    job_title = soup.find('h1').text
    # Organization
    organization = soup.find('a', href=True, target="_blank").text

    # Location
    location_list = [] # Create empty list to iterate reviews 
    # Only way found to access location is to go up the HTML Tree from organizaiton and iterate through. 
    parent_location = soup.find('a', href=True, target="_blank").parent.parent.parent.parent # Go up 4 levels to neccesary tags. 
    for item in parent_location: # Iterate through all items
        if "reviews" not in item.text and len(item.text) != 0: # Check if not Reviews or Empty
            location =item.text

    # Placed

    # Original Vacancy Url


write_vacancy_details('https://nl.indeed.com/viewjob?jk=bc45e0c30c4ce14f&q=Data+engineer&l=Enschede&tk=1g82vgbf8t1d0800&from=web&advn=8813502495358742&adid=383528202&ad=-6NYlbfkN0BQXeCo6iQsOwfgbj3luE1nO6L4Vj-ELuIdrTHanxZxHI4-9xKHVXue-MyuGV8G6_FvUQ0QJylCtdA8PEobwfG69EnHMJvsEsBARLe5ioD4SeO96C6_MDlPTkzrnbWEFb3qb0XwCw8WsZSDNceDTGnQisKYjWdByWQgO4FjKolALqGGx3KrN7s4Vr0cqcQgO2GjFlllFd0o4WfypwKOf_WAO4k0KqDOYtQCkkSGQa4LYz3vqDJy9MONm2VW-FaD89Db6A9G1XTDImLs5nJ9MoJ0SQwrWiqHonnv-mGEJqsRs2UvYhbRoyvSrFSYeH8mIyUlwAKT5KRrJl636-y6-5b5S4qxMmDoaZ-RlzGp-GvEfHh8cpnWx8zUls5JgGoTIAVjhJDdVsXSKQ%3D%3D&sjdu=4RLhhBSsAM57rxUjDStJRlrIetrQ-fUgEWmcIQQjBfCDGcOD5D3SC6zBySn3TDS1XySf2yPw9MOi8dkRfB_FOQxhqyZ971mqcawhFaU-xooHscHXUw1NIR0t1GG6nPwAYReB1mlge_CAtkDaHg3Go6g5uQl8PP8RjzBfINmFIxM&acatk=1g8335sfnjkn2802&pub=4a1b367933fd867b19b072952f68dceb&vjs=3')

write_vacancy_details('https://nl.indeed.com/viewjob?jk=f29ee9aae0e47e71&tk=1g832puvcjrgq803&from=serp&vjs=3&advn=5288612019923067&adid=364948793&ad=-6NYlbfkN0CaMwcIE_TkZUTeIdAT5IIiBYk61nE_g7Y-C7FX13p17SzfeBQwQ5CUHzAX8n9HoA3eBtJiL1Q7OTn4YzQlXR3LiGKBWTa_Zdq7q1gUkqWq01wBh_xh4iWwZ2OHFpR5HnFR2lambwWsxERoghqNkMcAtDgijTaA9Ma8HLNVc10RS1lL3q5mBt4uw_9n0QIaQCttlc3qfXA_Vxwwgguil8wxdo3IgpiK-mnSqJAqeCQAXocrzbITgZluUf0uvI4QQzV9FbacWaoBQH4unVGh2OToM_r0LL8V5i89TqgHlQcUAnl5Wecw3vonKLBJixHf3x1ZE0SW4v2LTZR1vcteM9IT9oeg_flOvy--EqQcz8PmxxFwK1fmxW8Q&sjdu=r_PaUU6fJg3c3GnYd8ltBMxfj2NrrNFLn790zfCdRp6plHtkcYy1HzSu2heMaw2_XAkfFoD4stLe1Iz2LAkw6jrA2cw13GLLfhnJCtVcUrbAn4-ZXbm4dxI-7x6J1tDNKIb6lU6CAnTBnWvN4kEeI2B5npp0dD6XC8LnQRdO-1M')

example_output_dict = {
    "Job_title": "Data Engineer", 
    "Organization": "HAYS",
    "Location": "Enschede",
    "Placed": "30+ dagen geleden geplaatst",
    "Original Vacancy Url": "https://m.hays.nl/Job/Detail/data-engineer-enschede-nl-NL_1044229",
    "Load_dts": datetime.today(),
    "LastSeen_dts": "",
    "Vacancy_hash": "",
    "Vacancy_text": "storytime"

}




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


