# Script to scrape Indeed and store in MongoDB
from pydoc import doc
from xmlrpc.client import DateTime
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
        self.database_name = self.jobname.replace(" ", "_") # for functions with spaces
    
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

                #sleep(randint(0,10)) # Add random waittime to avoid bot detection 

                # Getting Vacancies on current page
                soup = BeautifulSoup( get(url).content, "html.parser") # Reading Webpage into BeatifulSoup
                jobcards = soup.find('div', id = 'mosaic-provider-jobcards') # Locating part of webpage with only vacancies. 
                jobcards_headers = jobcards.find_all('h2') # find all Jobheaders (Titles)
                # Iterate over every Title to grab the url. 
                for header in jobcards_headers:
                    
                    #sleep(randint(0,10)) # Add random waittime to avoid bot detection

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
            # Set up MongoDB Connection
            client = MongoClient(self.MongoDB_connectionstring) 
            # Connect to right Database
            db = client[self.database_name]
            # Connect to STG collection
            collection = db.stg

            # Loop over Vacancies
            for vacancy in vacancies_urls:

                #sleep(randint(0,10)) # Add random waittime to avoid bot detection

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
                hasher = md5() # set up hash
                hasher.update(f"{job_title}~{organization}".encode("utf-8")) # Encode and Hash 
                vacancy_hash = hasher.hexdigest()

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
    
    def store(self):
        """
        PLACEHOLDER
        """
        from bson.objectid import ObjectId
        # Make MongoDB connection
        client = MongoClient(self.MongoDB_connectionstring)
        # Connect to right Database
        db = client[self.database_name]
        # Connect to DataStore collection (creates if not exists)
        datastore = db.DataStore
        # Connect to STG collection 
        stg = db.stg

        # Functions to be used
        def update_datastore(self):
            """
            Updates Datastore by inserting document which do not exist in collection. 
            While updating LastSeen_dts of the documents which are still present.
            """
            datastore_vacancy_hashes = [] # Empty list to add all existing Vacancy hashes
            for document in datastore.find(): datastore_vacancy_hashes.append(document["Vacancy_hash"]) # Append Vacancy Hash to list

            # Iterate over all documents in STG collection
            for document in stg.find():
                # Check if documents vacancy_hash is in the collection datastore
                if document["Vacancy_hash"] in datastore_vacancy_hashes:
                    # if vacancy_hash in datastore, then update LastSeen_dts to current
                    _id = ObjectId(document["_id"])
                    datastore.update_one(
                        {
                            "_id":_id
                        }, 
                        {"$set": 
                            {
                                "LastSeen_dts": datetime.today()
                            }
                        }
                        )
                else:
                    # Else insert document
                    datastore.insert_one(document)

        def empty_stg(self):
            """
            Clears the STG collection. 
            """
            try: # If STG is empty throws error. 
                stg.delete_many({})
            except Exception as e:
                print(f"Error: {e}")

        def mark_new(self):
            """
            If Vacancy has Load_dts of Today then marks this as new. 
            """
            # Iterate over all documents in the datastore, while grabbing only _id and Load_dts
            for document in datastore.find({}, {"_id": 1, "Load_dts":1}): 
                if document['Load_dts'].strftime("%Y-%m-%d") == datetime.today().strftime("%Y-%m-%d"): # Check if the days are the same
                    _id = ObjectId(document["_id"])
                    datastore.update_one(
                        {
                            "_id":_id
                        },
                        {"$set":
                            {
                                "New": True # If days are the same, set New to True
                            }
                        }
                    )
                else:
                    _id = ObjectId(document["_id"])
                    datastore.update_one(
                            {
                                "_id":_id
                            },
                            {"$set":
                                {
                                    "New": False # If days are not the same, set New to False
                                }
                            }
                        )
        
        def mark_status(self):
            """
            If Vacancy has LastSeen_dts of Today then marks status as active or inactive. 
            """
            # Iterate over all documents in the datastore, while grabbing only _id and LastSeen_dts
            for document in datastore.find({}, {"_id": 1, "LastSeen_dts":1}):
                _id = ObjectId(document["_id"])
                if document['LastSeen_dts'].strftime("%Y-%m-%d") == datetime.today().strftime("%Y-%m-%d"): #Check if the document has been seen today
                    # Create field Status and set to active
                    datastore.update_one(
                        {
                            "_id":_id
                        },
                        {
                            "$set":
                            {
                                "Status": "Active"
                            }
                        }
                    )
                else:
                    # Create field Status and set to inactive
                    datastore.update_one(
                        {
                            "_id":_id
                        },
                        {
                            "$set":
                            {
                                "Status": "Inactive"
                            }
                        }
                    )
        def delete_duplicates_datastore(self):
            """
            Iterates over Datastore collection to delete duplicate values based on Vacancy_hash. 
            """
            vacancy_hash_list = [] # Empty list to append to
            #Iterate over all documents in the datastore, while grabbing only _id and vacancy_hash
            for document in datastore.find({}, {"_id":1, "Vacancy_hash":1}):
                _id = ObjectId(document["_id"]) # set _id
                if document["Vacancy_hash"] not in vacancy_hash_list:
                    vacancy_hash_list.append(document["Vacancy_hash"])
                else:
                    datastore.delete_one(
                        {"_id":_id}
                    )
        
        # Call inner functions
        # First update the datastore collection with all the Documents and LastSeen_dts
        update_datastore(self)
        # Second make the vacancy hashes are unqiue. 
        delete_duplicates_datastore(self)
        # Third clear the STG Collections. 
        empty_stg(self)
        # Based on the Load_dts determine if Vacancy is new. 
        mark_new(self)
        # Based on LastSeen_dts determine if Vacancy is Active/Inactive
        mark_status(self)
    




if __name__ == "__main__":
    data_engineer = VacancyAnalysis("Data Engineer", "Enschede", "mongodb://localhost:27017")
    #data_engineer.extract()
    data_engineer.store()



