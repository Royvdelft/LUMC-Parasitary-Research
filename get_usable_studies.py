import sys
import requests
import json
import platform
import pandas as pd
from io import StringIO


# 
# -----
# This file retrieves studies from the ImmPort API based on a search term. 
# It then retrieves the file types and sizes for each study and decides if the study is usable based on the files.
# -----
# 


# Initialize dictionaries to store file type counts and file size aggregates per study
study_file_type_counts = {}
study_file_size_aggregates = {}
token = None

def main():

    # First we get the accesstoken
    global token
    total_studies_amt = 0
    passes_checks_amt = 0
    studies_that_pass = []
    username = ""
    password = ""

    token = generate_access_token(username, password)

    # Then fetch all the studies that match the search term
    response = fetch_studies_by_searchterm("malaria")

    # And for each study that matches, aggregate the file data
    for study in response:
        total_studies_amt += 1
        study_id = study['_id']
        response = get_study_files(study_id)

        aggregate_study_data(study_id, response)


    # Pick the studies that pass the aggregation check
    studies = filter_studies_with_files()

    # For each study that passes the check, get the study info
    for study_id in studies:
        study_data = get_study_info(study_id)
        passes_template_check = template_check(study_data)

        if(passes_template_check):
            passes_checks_amt += 1
            studies_that_pass.append(study_id)

    print(f"Studies that pass the template check: {studies_that_pass}")
    print(f" --- ")
    print(f"Total studies: {total_studies_amt}")
    print(f"Studies that pass the template check: {passes_checks_amt}")
    percentage = (passes_checks_amt / total_studies_amt) * 100
    print(f"Percentage of studies that pass the template check: {percentage}%")



def generate_access_token(username, password):
    # Send a POST request to obtain the access token
    response = requests.post("https://www.immport.org/auth/token", data={"username": username, "password": password})

    # Extract the access token from the response
    token = response.json()["access_token"]

    return token


def fetch_studies_by_searchterm(search_term):
    req = requests.get(f'https://www.immport.org/shared/data/query/api/search/study?term={search_term}&pageSize=100&fromRecord=0&sortField=&sortFieldDirection=', headers={
        'Content-Type': "application/json"
    })

    # Assuming data is the response object containing JSON content
    json_data = json.loads(req.content)
    # Convert the JSON data to a string and wrap it in a StringIO object (got a FutureWarning if I didn't)
    # json_io = StringIO(json.dumps(json_data))

    return json_data['hits']['hits']


def get_study_files(study_id):
    global token

    # Use the access token in a subsequent request
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"https://www.immport.org/data/query/result/filePath?format=csv&studyAccession={study_id}", headers=headers)


    if response.status_code == 200:
        # Parse response as JSON
        json_data = json.loads(response.content)
        # print(json_data)
        return json_data
    else:
        sys.exit(1)


def aggregate_study_data(study_id, file_data):
    # Initialize dictionaries to store file type counts and file size aggregates for the current study
    global study_file_type_counts
    global study_file_size_aggregates

    # Initialize dictionaries to store file type counts and file size aggregates for the current study
    file_type_counts = {}
    file_size_aggregates = {}

    for file in file_data:
        # Get the file type from the file name extension
        file_type = file['fileName'].split('.')[-1]
        file_size = file['filesizeBytes']
        file_type_counts[file_type] = file_type_counts.get(file_type, 0) + 1
        file_size_aggregates[file_type] = file_size_aggregates.get(file_type, 0) + file_size

    # Store the file type counts and file size aggregates for the current study
    study_file_type_counts[study_id] = file_type_counts
    study_file_size_aggregates[study_id] = file_size_aggregates

    print(study_id + " processed successfully")


def filter_studies_with_files():
     # For each object in study file type counts, check if has any key with value of more than 0
    studies = []
    global study_file_type_counts

    for study_id, file_type_counts in study_file_type_counts.items():
        if any(count > 0 for count in file_type_counts.values()):
            studies.append(study_id)

    return studies


def get_study_info(study_id):
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"https://www.immport.org/data/query/api/study/{study_id}", headers=headers)
    # print(study)

    if response.status_code == 200:
        # Parse response as JSON
        study_data = response.json()[0]
        return study_data
    else:
        sys.exit(1)


def template_check(study_data):

    # if has a brief description, actual start date, actual completion date, and actual enrollment and the values are not None
    if (study_data['briefDescription'] is not None 
        and study_data['actualStartDate'] is not None 
        and study_data['actualCompletionDate'] is not None 
        and study_data['actualEnrollment'] is not None
        and study_data['endpoints'] is not None):
        return True

    return False


if __name__ == "__main__":
    main()