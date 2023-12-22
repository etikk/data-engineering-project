import requests
import json
import os
from Get_Samples_For_Citations import extract_values

# List of titles for the articles you're interested in
titles = extract_values('categories', 'title')
#print(titles)

# Base URL for the Crossref API
base_url = "https://api.crossref.org/works/"

# Empty dictionary to store the results
results = {}

# Loop through the titles
for title in titles:
    # Send a GET request to the Crossref API to search for the DOI
    doi_response = requests.get(base_url, params={"query.title": title})
    
    # Parse the response as JSON
    doi_data = doi_response.json()
    
    # Extract the DOI of the first result
    doi = doi_data['message']['items'][0]['DOI']
    
    # Send another GET request to the Crossref API to get the 'is-referenced-by-count'
    count_response = requests.get(base_url + doi)
    
    # Parse the response as JSON
    count_data = count_response.json()
    
    # Extract the 'is-referenced-by-count' and store it in the results dictionary
    results[title] = count_data['message']['is-referenced-by-count']

# Create directory if it doesn't exist
if not os.path.exists('Enriched_Data'):
    os.makedirs('Enriched_Data')

# Write the results to a JSON file in the new directory
with open('Enriched_Data/Nbr_Of_Citations.json', 'w') as f:
    json.dump(results, f)
