# Vacancy-Analysis
## **Architecture**
<img width="4896" alt="Vacancy-Analysis Architecture" src="https://user-images.githubusercontent.com/73876003/182046827-c91368fe-7dc3-45ba-9e3b-8d08ea8f4015.png">
This project scrapes Indeed vacancies with the help of Python (Requests & Beautiful Soup).
<br>These vacancies are then stored in Mongodb and useable for the Discord Bot.

## **ETL** 
Through the use of the libaries Requests & Beautiful Soup all the vacancies are extracted from Indeed.<br>
ETL process uses a Docker container. 
<br>
<br>
To edit for personal use, please change functions_list and locations_list in the file etl/extract.py

## **Database**
Instance of MongoDB through the use of Docker. For every Function (in functions_list) a database is created in MongoDB. 
Every database has three collections:
- STG        --> STG is used for updating DataStore and is cleared after every run.
- DataStore  --> DataStore is based on DataVault2 principles, where Vacancy_hash is determined by hashing Function and Organization. 
- Analysis   --> Analysis contains documents used by the Discord Bot. 


## **Bot**
Bot for Discord Server through the use of Docker.
Discord Server has for every Function seperate channel. 
Bot has multiple commands:
- $new      --> Sends for every new vacancy the Function, Organization, Location and URL (Message is in Dutch)
- $summary  --> Sends Bar chart containing Total Amount of Vacancies, Active Vacancies and New Vacancies. 
- $location --> Sends Horizontal Bar chart containing Location and count of Vacancies. 
- $skills   --> Sends Bar chart containing the count of vacancies mentioning Cloud Provider (Azure, AWS, Google) and specific skills (Python, SQL)

