# Script to extract
from Vacancy_Analysis import VacancyAnalysis # Import created Class

connection_string_local = "mongodb://localhost:27017"
connection_string_docker = "mongodb://MongoDB_Container"

# Define functions to look for 
functions_list = ["Data Engineer", "Data Analist", "Business Intelligence", "Product Owner"]
# Define Locations 
locations_list = ["Enschede", "Almelo", "Deventer", "Zwolle"]


if __name__ == "__main__":
    for function in functions_list:
        for location in locations_list:
            vacancy = VacancyAnalysis(jobname=function, location=location, MongoDB_connectionstring=connection_string_docker) # Create instance of Vacancyanalis
            # Extract all elements
            vacancy.extract() # 
            # Store all elements
            vacancy.store()
            # Create Analysis
            vacancy.analyze()
            print(function, location)