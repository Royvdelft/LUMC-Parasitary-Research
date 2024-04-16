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

def main():

    # Get usable malaria studies
    response = fetch_studies("malaria")

    for study in response:
        study_id = study['_id']
        response = get_study(study_id)

        aggregate_study_data(study_id, response)


    studies = pick_usable_studies()
    print('usable studies', studies)


def fetch_studies(search_term):
    req = requests.get(f'https://www.immport.org/shared/data/query/api/search/study?term={search_term}&pageSize=100&fromRecord=0&sortField=&sortFieldDirection=', headers={
        'Content-Type': "application/json"
    })

    # Assuming data is the response object containing JSON content
    json_data = json.loads(req.content)
    # Convert the JSON data to a string and wrap it in a StringIO object (got a FutureWarning if I didn't)
    # json_io = StringIO(json.dumps(json_data))

    return json_data['hits']['hits']


def get_study(study_id):
    print(study_id)

    # Send a POST request to obtain the access token
    response = requests.post("https://www.immport.org/auth/token", data={"username": "davidfraterman", "password": "D228cw75@"})

    # Extract the access token from the response
    token = response.json()["access_token"]

    # Use the access token in a subsequent request
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"https://www.immport.org/data/query/result/filePath?format=csv&studyAccession={study_id}", headers=headers)

    if response.status_code == 200:
        # Parse response as JSON
        json_data = json.loads(response.content)

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


def pick_usable_studies():
     # For each object in study file type counts, check if has any key with value of more than 0
    studies = []
    global study_file_type_counts

    for study_id, file_type_counts in study_file_type_counts.items():
        if any(count > 0 for count in file_type_counts.values()):
            studies.append(study_id)

    return studies


if __name__ == "__main__":
    main()