#from scholarly import scholarly
#from scholarly import ProxyGenerator
# Retrieve the author's data, fill-in, and print
# Get an iterator for the author results
#search_query = scholarly.search_pubs('Quantum Group of Isometries in Classical and Noncommutative Geometry')
# Retrieve the first result from the iterator
import requests
import json

# List of titles for the articles you're interested in
titles = ["Quantum Group of Isometries in Classical and Noncommutative Geometry"]

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

# Write the results to a JSON file
with open('results.json', 'w') as f:
    json.dump(results, f)
#scholarly.pprint(next(search_query))


"""
pg = ProxyGenerator()
pg.FreeProxies()
scholarly.use_proxy(pg)

search_query = scholarly.search_pubs('Quantum Group of Isometries in Classical and Noncommutative Geometry')
scholarly.pprint(next(search_query))
"""
"""
first_author_result = next(search_query)
scholarly.pprint(first_author_result)

# Retrieve all the details for the author
author = scholarly.fill(first_author_result )
scholarly.pprint(author)

# Take a closer look at the first publication
first_publication = author['publications'][0]
first_publication_filled = scholarly.fill(first_publication)
scholarly.pprint(first_publication_filled)

# Print the titles of the author's publications
publication_titles = [pub['bib']['title'] for pub in author['publications']]
print(publication_titles)

# Which papers cited that publication?
citations = [citation['bib']['title'] for citation in scholarly.citedby(first_publication_filled)]
print(citations)
"""