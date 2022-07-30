# Script to extract
from Vacancy_Analysis import VacancyAnalysis # Import created Class
import schedule # For scheduling the job. 
import time

#Create extract_job for all the extractions
def extract_job():
    # Define parameters for the extraction
    functions_list = ["Data Engineer", "Data Analist", "Business Intelligence", "Product Owner"]
    locations_list = ["Enschede", "Almelo", "Deventer", "Zwolle"]
    for function in functions_list:
        for location in locations_list:
            vacancy = VacancyAnalysis(jobname=function, location=location, MongoDB_connectionstring="mongodb://localhost:27017") # Create instance of Vacancyanalis
            # Extract all elements
            vacancy.extract() # 
            # Store all elements
            vacancy.store()
            # Create Analysis
            vacancy.analyze()
            print(function, location)


extract_job()
# schedule.every().day.at("06:00").do(extract_job)

# while True:
#     schedule.run_pending()
#     time.sleep(10800)
