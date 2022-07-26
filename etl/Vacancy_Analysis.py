# Script to scrape Indeed and store in MongoDB
from types import NoneType
from pymongo import MongoClient # For writing to MongoDB
from bs4 import BeautifulSoup
import pymongo
from requests import get
from random import randint
from time import sleep
from datetime import datetime
from hashlib import md5
import logging


# Set up Logging Basicconfig
logging.basicConfig(filename="Vacancy.log",
                    format="%(asctime)s-%(levelname)s-%(message)s"
                    )
# Set up Logger
logger = logging.getLogger("Vacancy")
logger.setLevel(logging.INFO)

class VacancyAnalysis:

    def __init__(self, jobname:str, location:str, MongoDB_connectionstring:str):
        self.jobname = jobname
        self.location = location
        self.MongoDB_connectionstring = MongoDB_connectionstring
        self.database_name = self.jobname.replace(" ", "_") # for functions with spaces
        logger.info(f"Created with Jobname: '{self.jobname}'| Location: '{self.location}'| Connectionstring: '{self.MongoDB_connectionstring}'")

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
            logger.info(f"Created URL: {job_url}")
            return job_url
        
        def get_all_vacancies(self, url:str):
            vacancies_url = [] # Empty list to append sub-urls. 
            next_page_exits = True # Variable to control while loop
            counter = 1 # Counter to control webpage selection
            page_dict = {} # Empty Dict to prevent exiting with error. 

            logger.info("Iterating over all pages to get all urls")
            while next_page_exits: # To go over all webpages
                logger.info(f"Iteration: {counter}| URL: {url}")

                #sleep(randint(0,10)) # Add random waittime to avoid bot detection 

                # Getting Vacancies on current page
                soup = BeautifulSoup( get(url).content, "html.parser") # Reading Webpage into BeatifulSoup
                jobcards = soup.find('div', id = 'mosaic-provider-jobcards') # Locating part of webpage with only vacancies. 

                # Check if Jobcards is not NoneType
                if isinstance(jobcards, NoneType):
                    logger.warning(f"Failed to retrieve list of Vacancies for {self.jobname} and {self.location} on iteration {counter}")
                    break # Break out of loop
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
                except Exception as e: # If fails then log exception
                    logger.warning(f"While loop exited with following exception: {e}")
                    next_page_exits = False
            logger.info("Retrieved all urls")
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
            logger.info("Writing vacancy details to MongoDB STG collection")
            # Loop over Vacancies
            for vacancy in vacancies_urls:
                logger.info(f"Starting {vacancy}")
                #sleep(randint(0,10)) # Add random waittime to avoid bot detection

                # Read in Vacancy to Soup    
                soup = BeautifulSoup( get(vacancy).content, "html.parser")

                # Title
                try:
                    job_title = soup.find('h1').text
                except Exception as e:
                    logger.warning(f"Could not find Job Title| Exception: {e}")
                    break # Break out of loop and do not process vacancy

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
                    if element.text.__contains__("geleden") or element.text.__contains__("geplaatst"):
                            placed = element.text
                
                # Original Vacancy Url
                try: # try and except blok because not always present
                    orginal_vacancy_url_element = soup.find('a', {"target":"_blank", "rel": "noopener", "href":True})
                    orginal_vacancy_url = orginal_vacancy_url_element['href']
                except Exception as e:
                    logger.warning(f"Oringal Vacancy Url not found| Exception {e}")
                    orginal_vacancy_url = ""

                #Vacancy_hash
                hasher = md5() # set up hash
                hasher.update(f"{job_title}~{organization}".encode("utf-8")) # Encode and Hash 
                vacancy_hash = hasher.hexdigest()

                #Vacancy_text
                vacancy_text_raw = soup.find('div', {"id":"jobDescriptionText", "class":"jobsearch-jobDescriptionText"})
                vacancy_text = vacancy_text_raw.text

                #Construct JSON output
                try:
                    # Try block to prevent issues with UnboundLocalError
                    vacancy_document = {
                        "Job_title": job_title, 
                        "Organization": organization,
                        "Location": location,
                        "Placed": placed,
                        "Original Vacancy Url": orginal_vacancy_url,
                        "Load_dts": datetime.today(),
                        "LastSeen_dts": datetime.today(),
                        "Vacancy_hash": vacancy_hash,
                        "Vacancy_text": vacancy_text,
                        "URL": vacancy
                    }
                except UnboundLocalError:
                    logger.warning(f"Could not create document due to UnboundLocalError")
                    break # Find out what goes wrong to fix it (add to log)
                collection.insert_one(vacancy_document) # insert in the STG collection
                logger.info(f"Successfully inserted {vacancy_hash}| {job_title}| {organization}") # Convert to logging
            logger.info("Done with writing Vacancy Details")
        
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
            logger.info(f"Start Updating Datastore")
            datastore_vacancy_hashes = {} # Empty dict to add all existing Vacancy hashes
            for document in datastore.find(): datastore_vacancy_hashes[document["Vacancy_hash"]] = document["_id"] # Dict with Hash as Key and _ID as value
            # Iterate over all documents in STG collection
            for document in stg.find():
                logger.info(f"Updating {document['Vacancy_hash']}|{document['Job_title']}|{document['Organization']}")
                # Check if documents vacancy_hash is in the collection datastore
                if document["Vacancy_hash"] in datastore_vacancy_hashes.keys():
                    logger.info(f"'{document['Vacancy_hash']}' found in DataStore")
                    # if vacancy_hash in datastore, then update LastSeen_dts to current
                    _id = ObjectId(datastore_vacancy_hashes[document['Vacancy_hash']]) # Returning the _ID
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
                    logger.info(f"'{document['Vacancy_hash']}' inserted in DataStore")
                    # Else insert document
                    datastore.insert_one(document)
            logger.info(f"Done Updating Datastore")

        def empty_stg(self):
            """
            Clears the STG collection. 
            """
            logger.info("Empty STG Collection")
            try: # If STG is empty throws error. 
                stg.delete_many({})
            except Exception as e:
                logger.warning(e)

        def mark_new(self):
            """
            If Vacancy has Load_dts of Today then marks this as new. 
            """
            # Iterate over all documents in the datastore, while grabbing only _id and Load_dts
            logger.info("Started Marking New documents")
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
            logger.info("Finished Marking New documents")
        
        def mark_status(self):
            """
            If Vacancy has LastSeen_dts of Today then marks status as active or inactive. 
            """
            logger.info("Started Marking Status")
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
            logger.info("Finished Marking Status")

        def delete_duplicates_datastore(self):
            """
            Iterates over Datastore collection to delete duplicate values based on Vacancy_hash. 
            """
            logger.info("Started Deleting Duplicates in Datastore")
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
            logger.info("Finished Deleting Duplicates in Datastore")

        def add_vacancy_age(self):
            """
            Function to add information about the Vacancy_age based on Load_dts and Placed. 
            """
            logger.info("Started adding Vacancy Age")
            #Iterate over all documents in the datastore, while grabbing only _id, load_dts and placed
            for document in datastore.find({}, {"_id":1, "Load_dts":1, "Placed":1}):
                # Extract Number of Days placed from Placed
                if "+" in document['Placed'].split()[0]: # Check if vacancy is placed 30+ days ago
                    placed_num_days = 30
                elif document['Placed'].split()[0] == "Vandaag" or document['Placed'].split()[0] == "Zojuist": # Check if vacancy is placed today
                    placed_num_days = 0
                else: # Else a Int can be extracted. 
                    placed_num_days = int(document['Placed'].split()[0])
                
                # Calculate days between Load_dts and Today()
                days_between_load_dts_today = datetime.today() - document["Load_dts"]

                # Update Documents
                _id = ObjectId(document["_id"]) # set _id
                datastore.update_one(
                    {
                        "_id":_id
                    },
                    {
                        "$set":
                        {
                            "Vacancy_age": int(days_between_load_dts_today.days) + placed_num_days
                        }
                    }
                )
            logger.info("Finished adding Vacancy Age")

        def convert_location_to_placename(self):
            """
            Function to convert location of vacancy to valid Dutch place. 
            """
            logger.info("Started Converting Locations")
            # iterate over all documents in the datastore
            for document in datastore.find({"Location": {"$regex": "in"}}, {"_id": 1, "Location": 1}): # Iterate over all documents and find with Location in
                # For Locations with in mostly last word is the placename, only exception is "Hengelo OV"
                if document['Location'].split(" ")[-1] == "OV": # In case Hengelo OV the last word is OV
                    new_location_string = document['Location'].split(" ")[-2]
                else: # For other Location string last place is correct
                    new_location_string =  document['Location'].split(" ")[-1]# Split the location on space and grab last word (placename)
                
                # Update document
                _id = ObjectId(document["_id"]) # Set _ID
                datastore.update_one(
                    {
                        "_id":_id
                    },
                    {
                        "$set":
                        {
                            "Location": new_location_string
                        }
                    }
                )
            logger.info("Finished Converting Locations")
        
        # Call inner functions
        # First update the datastore collection with all the Documents and LastSeen_dts
        update_datastore(self)
        # Second make the vacancy hashes are unique. 
        delete_duplicates_datastore(self)
        # Third clear the STG Collections. 
        empty_stg(self)
        # Based on the Load_dts determine if Vacancy is new. 
        mark_new(self)
        # Based on LastSeen_dts determine if Vacancy is Active/Inactive
        mark_status(self)
        # Based on Placed and Load_dts calculate vacancy_age
        add_vacancy_age(self)
        # Convert Location to normal place names
        convert_location_to_placename(self)
    
    def analyze(self):
        """
        PLACEHOLDER
        """
        # Set up Connection
        from bson.objectid import ObjectId
        # Make MongoDB connection
        client = MongoClient(self.MongoDB_connectionstring)
        # Connect to right Database
        db = client[self.database_name]
        # Connect to DataStore collection (creates if not exists)
        datastore = db.DataStore
        # Connect to Analysis collection (creates if not exists)
        analysis = db.Analysis

        # Inner functions
        def summary_statistics(self):
            """
            Creates or replace the document called Summary Statistics of the collection db.Analysis
            """
            logger.info("Started Creating Summary Statistics")
            # Create document
            document = {"Title":"summary statistics"}

            # Measures for in the document
            # Vacancy_count
            vacancy_count = datastore.count_documents(filter={}) # Retrieve Count of documents
            document["Vacancy Count"] = vacancy_count # set to document

            # Active Count
            count_active = datastore.count_documents({"Status":"Active"}) # Retrieve count of all active documents
            document["Active Count"] = count_active

            # New Count
            count_new = datastore.count_documents({"New":True}) # Retrieve count of all new documents
            document["New Count"] = count_new

            # Average Vacancy Age
            avg_vacancy_age = datastore.aggregate([ # Retrieve the Average Vacancy Age
                {
                    "$match": {"Status": "Active"}
                },
                {
                    "$group":
                    {
                        "_id": "null",
                        "Average_Vacancy_Age": {"$avg": "$Vacancy_age" }
                    }
                },
                {
                    "$project":
                    {
                        "_id": 0,
                        "Average_Vacancy_Age": 1
                    }
                }
            ]
            )
            for item in avg_vacancy_age: document["Average Vacancy Age"] = round(item['Average_Vacancy_Age']) # Assign with for loop

            # Min Vacancy Age
            min_vacancy_age = datastore.aggregate([ # Retrieve the Min Vacancy Age
                {
                    "$match": {"Status": "Active"}
                },
                {
                    "$group":
                    {
                        "_id": "null",
                        "Min_Vacancy_Age": {"$min": "$Vacancy_age" }
                    }
                },
                {
                    "$project":
                    {
                        "_id": 0,
                        "Min_Vacancy_Age": 1
                    }
                }
            ]
            )
            for item in min_vacancy_age: document["Min Vacancy Age"] = round(item['Min_Vacancy_Age']) # Assign with for loop

            # Max Vacancy Age
            max_vacancy_age = datastore.aggregate([ # Retrieve the Min Vacancy Age
                {
                    "$match": {"Status": "Active"}
                },
                {
                    "$group":
                    {
                        "_id": "null",
                        "Max_Vacancy_Age": {"$max": "$Vacancy_age" }
                    }
                },
                {
                    "$project":
                    {
                        "_id": 0,
                        "Max_Vacancy_Age": 1
                    }
                }
            ]
            )
            for item in max_vacancy_age: document["Max_Vacancy_Age"] = round(item['Max_Vacancy_Age']) # Assign with for loop

            # Find document for insert/replace
            if isinstance(analysis.find_one({"Title":"summary statistics"}), NoneType):
                # Document does not exist
                # Insert statement
                analysis.insert_one(document)
            else:
                # Document does exist 
                # Replace statement
                analysis.replace_one(
                    {
                        "Title": "summary statistics"
                    },
                    document
                )
            logger.info("Finished Adding summary statistics")
        
        def location_statistics(self):
            """
            Retrieves all Vacancies per Location.
            """
            # Create document
            document = {"Title":"location statistics"}

            # Measure
            location_count = datastore.aggregate([
                {
                    "$match": {"Status": "Active"}
                },
                {
                    "$group":
                    {
                        "_id": "$Location",
                        "count" : {"$sum": 1}
                    }
                },
                {
                   "$project":
                    {
                        "_id": 1,
                        "count": 1
                    } 
                }
            ])

            # Find document for insert/replace
            if isinstance(analysis.find_one({"Title":"location statistics"}), NoneType):
                # Document does not exist
                #Create documentSet up connectionistics"}
                for item in location_count: document[item['_id']] = item["count"] # For loop to add items to document
                # Insert statement
                analysis.insert_one(document)
            else:
                # Document does exist 
                # Iterate over all elements
                for item in location_count:
                    # update statement
                    analysis.update_one(
                        {
                            "Title": "location statistics"
                        },
                        {
                            "$set":
                            {
                                item['_id'] : item["count"]
                            }
                        }
                    )
        
        def skills_statistics(self):
            """
            Retrieves the count of certain skills in all vacancies.
            """
            # Create Document
            document = {"Title":"skills statistics"}
            # Retrieve the cloud environment
            # The function of Product Owner is different then Data Analist or Data Engineer
            if self.jobname.lower() != "product owner":
                # Retrieve the right Cloud
                # Google
                _google_count = 0
                _google = datastore.find({"Vacancy_text": {"$regex": "google", "$options": 'i'}}) # Amount of Vacancies with google
                for vacancy in _google: _google_count += 1

                # AWS
                _aws_count = 0
                _aws = datastore.find({"Vacancy_text": {"$regex": "aws", "$options": 'i'}}) # Amount of Vacancies with aws
                for vacancy in _aws: _aws_count += 1

                # Azure
                _azure_count = 0
                _azure = datastore.find({"Vacancy_text": {"$regex": "azure", "$options": 'i'}}) # Amount of Vacancies with azure
                for vacancy in _azure: _azure_count += 1

                # Add to Document
                document["Google Count"] = _google_count
                document["AWS Count"] = _aws_count
                document["Azure Count"] = _azure_count

                # Retrieve Python, SQL skills
                _sql_count = 0
                _sql = datastore.find({"Vacancy_text": {"$regex": "sql", "$options": 'i'}}) # Amount of Vacancies with sql
                for vacancy in _sql: _sql_count += 1

                _python_count = 0
                _python = datastore.find({"Vacancy_text": {"$regex": "python", "$options": 'i'}}) # Amount of Vacancies with python
                for vacancy in _python: _python_count += 1

                # Add to Document
                document["SQL Count"] = _sql_count
                document["Python Count"] = _python_count
            
             # Find document for insert/replace
            if isinstance(analysis.find_one({"Title":"skills statistics"}), NoneType):
                # Document does not exist
                # Insert statement
                analysis.insert_one(document)
            else:
                # Document does exist 
                # Replace statement
                analysis.replace_one(
                    {
                        "Title": "skills statistics"
                    },
                    document
                )

        summary_statistics(self)
        location_statistics(self)
        skills_statistics(self)


if __name__ == "__main__":
    vacancy = VacancyAnalysis("Data Engineer", "Enschede", "mongodb://localhost:27017")
    vacancy.store()

