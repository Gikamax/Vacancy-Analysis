# Script to scrape Indeed and store in MongoDB
from pymongo import MongoClient # For writing to MongoDB
from bs4 import BeautifulSoup
import requests
import json
import pprint
from random import randint
from time import sleep
from datetime import datetime
import hashlib

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
    next_page_exits = True # Variable to control while loop
    counter = 1 # Counter to control webpage selection
    page_dict = {} # Empty Dict to prevent exiting with error. 

    while next_page_exits: # To go over all webpages
        # Getting Vacancies on current page
        soup = BeautifulSoup( requests.get(url).content, "html.parser") # Reading Webpage into BeatifulSoup
        jobcards = soup.find('div', id = 'mosaic-provider-jobcards') # Locating part of webpage with only vacancies. 
        jobcards_headers = jobcards.find_all('h2') # find all Jobheaders (Titles)
        # Iterate over every Title to grab the url. 
        for header in jobcards_headers:
            if "data" in header.text.lower(): # Filter Vacancies that have Data in it. 
                url = "https://nl.indeed.com" + header.find('a')['href'] # Add base url to href output. 
                vacancies_url.append(url) # add found url to list
        
        #Finding next webpage
        parent_navigation = soup.find('ul', {"class": "pagination-list"}) # Find Navigation panel on bottom of the page
        for element in parent_navigation: # Loop through all possible values
            if element.text != "\n" and element.text != "" and int(element.text) != counter: # Text can be \n, " " and equal to current page, these must be avoided
                page_dict[int(element.text)] = "https://nl.indeed.com" + element.find('a', href = True)['href'] # Add to Dic Pagenumber with correct Url 
        counter += 1 # Set counter +1
        try: # Try setting new url to new page
            url = page_dict[counter] 
        except Exception as e: # If fails then print exception
            print(e)
            next_page_exits = False
        
    return vacancies_url # return list. 

def write_vacancy_details(function:str, vacancies_urls:list, MongoDB_connectionstring:str):
    """
    Grabs all vacancy details and writes them to MongoDB
    """
    database_name = function.replace(" ", "_") # for functions with spaces
    # Set up MongoDB Connection
    client = MongoClient(MongoDB_connectionstring) 
    # Connect to right Database
    db = client[database_name]
    # Connect to STG collection
    collection = db.stg
    for vacancy in vacancies_urls:
            
        soup = BeautifulSoup( requests.get(vacancy).content, "html.parser")

        # Title
        job_title = soup.find('h1').text

        # Organization
        organization = soup.find('a', href=True, target="_blank").text

        # Location
        parent_location = soup.find('div', {"class":"icl-u-xs-mt--xs icl-u-textColor--secondary jobsearch-JobInfoHeader-subtitle jobsearch-DesktopStickyContainer-subtitle"})
        for element in parent_location:
            if "reviews" not in element.text and len(element.text) != 0:
                location = element.text

        # Placed
        parent_placed = soup.find('div', {"class": "jobsearch-JobMetadataFooter"})
        for element in parent_placed:
            if element.text.__contains__("geleden"):
                placed = element.text
        
        # Original Vacancy Url
        try: # try and except blok because not always present
            orginal_vacancy_url_element = soup.find('a', {"target":"_blank", "rel": "noopener", "href":True})
            orginal_vacancy_url = orginal_vacancy_url_element['href']
        except Exception as e:
            orginal_vacancy_url = ""

        #Vacancy_hash
        md5 = hashlib.md5() # set up hash
        md5.update(f"{job_title}~{organization}".encode("utf-8")) # Encode and Hash 
        vacancy_hash = md5.hexdigest()

        #Vacancy_text
        vacancy_text_raw = soup.find('div', {"id":"jobDescriptionText", "class":"jobsearch-jobDescriptionText"})
        vacancy_text = vacancy_text_raw.text

        #Construct JSON output
        vacancy_document = {
            "Job_title": job_title, 
            "Organization": organization,
            "Location": location,
            "Placed": placed,
            "Original Vacancy Url": orginal_vacancy_url,
            "Load_dts": datetime.today(),
            "LastSeen_dts": datetime.today(),
            "Vacancy_hash": vacancy_hash,
            "Vacancy_text": vacancy_text
        }
        collection.insert_one(vacancy_document) # insert in the STG collection
    print("Succes")

if __name__ == "__main__":
    url = create_url("Data Engineer", "Enschede")
    url_list = get_all_vacancies(url)
    write_vacancy_details("Data Engineer", url_list, "mongodb://localhost:27017")



