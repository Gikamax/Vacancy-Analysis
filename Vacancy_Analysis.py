# Script to scrape Indeed and store in MongoDB
from pymongo import MongoClient # For writing to MongoDB
from bs4 import BeautifulSoup
from requests import get
from random import randint
from time import sleep
from datetime import datetime
from hashlib import md5

class VacancyAnalysis:

    def __init__(self, jobname:str, location:str, MongoDB_connectionstring:str):
        self.jobname = jobname
        self.location = location
        self.MongoDB_connectionstring = MongoDB_connectionstring
    
    def extract(self):
        """
        PLACEHOLDER
        """
        # Define inner functions to be used

        def create_url(self) -> str:
            """
            Function to generate indeed Url based on Jobname and Location. 
            Accepts Jobname with spaces
            """
            jobname_without_spaces = str(self.jobname).replace(' ', "%20")
            job_url = f"https://nl.indeed.com/jobs?q={jobname_without_spaces}&l={self.location}"
            return job_url
        
        def get_all_vacancies(self, url:str):
            vacancies_url = [] # Empty list to append sub-urls. 
            next_page_exits = True # Variable to control while loop
            counter = 1 # Counter to control webpage selection
            page_dict = {} # Empty Dict to prevent exiting with error. 

            while next_page_exits: # To go over all webpages

                sleep(randint(0,10)) # Add random waittime to avoid bot detection 

                # Getting Vacancies on current page
                soup = BeautifulSoup( get(url).content, "html.parser") # Reading Webpage into BeatifulSoup
                jobcards = soup.find('div', id = 'mosaic-provider-jobcards') # Locating part of webpage with only vacancies. 
                jobcards_headers = jobcards.find_all('h2') # find all Jobheaders (Titles)
                # Iterate over every Title to grab the url. 
                for header in jobcards_headers:
                    
                    sleep(randint(0,10)) # Add random waittime to avoid bot detection

                    if "data" in header.text.lower(): # Filter Vacancies that have Data in it. 
                        url = "https://nl.indeed.com" + header.find('a')['href'] # Add base url to href output. 
                        vacancies_url.append(url) # add found url to list
                
                #Finding next webpage
                parent_navigation = soup.find('ul', {"class": "pagination-list"}) # Find Navigation panel on bottom of the page
                try:
                    for element in parent_navigation: # Loop through all possible values
                        if element.text != "\n" and element.text != "" and int(element.text) != counter: # Text can be \n, " " and equal to current page, these must be avoided
                            page_dict[int(element.text)] = "https://nl.indeed.com" + element.find('a', href = True)['href'] # Add to Dic Pagenumber with correct Url 
                    counter += 1 # Set counter +1
                    try: # Try setting new url to new page
                        url = page_dict[counter] 
                    except:
                        pass
                except Exception as e: # If fails then print exception
                    next_page_exits = False
                
            return vacancies_url # return list. 
        
        def write_vacancy_details(self, vacancies_urls:list):
            """
            Grabs all vacancy details and writes them to MongoDB
            """
            database_name = self.jobname.replace(" ", "_") # for functions with spaces
            # Set up MongoDB Connection
            client = MongoClient(self.MongoDB_connectionstring) 
            # Connect to right Database
            db = client[database_name]
            # Connect to STG collection
            collection = db.stg

            # Loop over Vacancies
            for vacancy in vacancies_urls:

                sleep(randint(0,10)) # Add random waittime to avoid bot detection

                # Read in Vacancy to Soup    
                soup = BeautifulSoup( get(vacancy).content, "html.parser")

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
                md5 = md5() # set up hash
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
                print(f"Successfly instered {job_title}")
        
        # Execute inner functions
        self.job_url = create_url(self)
        self.list_vacancies = get_all_vacancies(self, self.job_url)
        write_vacancy_details(self, self.list_vacancies)
    
        

if __name__ == "__main__":
    data_engineer = VacancyAnalysis("Data Engineer", "Enschede", "mongodb://localhost:27017")
    data_engineer.extract()



