import pandas as pd
import requests
import os 
import pandas as pd
import json

# Assuming you have a DataFrame df with the 'categories' and 'title' columns
def extract_values(cat, titl):
    directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'raw_data')
    results = []
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            if filename == 'chunk_1.json':
                with open(os.path.join(directory, filename), 'r') as f:
                    data = json.load(f)
                    df = pd.json_normalize(data)
                    if cat in df.columns and titl in df.columns:
                        categories = df[cat].unique()
                        for category in categories:
                            if category not in results:
                                results.append(df[df[cat] == category][titl].iloc[0])
                            if len(results) >= 300:
                                return results
    return results

# Extract unique titles from the 'title' column
titles = extract_values('categories', 'title')

# Base URL for the Crossref API
base_url = "https://api.crossref.org/works/"

# Create an empty DataFrame to store the results
results_df = pd.DataFrame(columns=['Title', 'ReferencedByCount','PublicationType'])

batch_size = 10

for i in range(0, len(titles), batch_size):
    batch_titles = titles[i:i + batch_size]

    # Send a GET request to the Crossref API to search for DOIs in batch
    doi_response = requests.get(base_url, params={"query.title": ",".join(batch_titles)})
    
    # Parse the response as JSON
    doi_data = doi_response.json()
    
    # Loop through the items in the response
    for item in doi_data['message']['items']:
        # Extract DOI and send another GET request for 'is-referenced-by-count'
        doi = item.get('DOI', None)
        count_response = requests.get(base_url + doi)
        
        # Parse the response as JSON
        count_data = count_response.json()
        
        # Extract the 'is-referenced-by-count' and 'type'
        referenced_by_count = count_data['message'].get('is-referenced-by-count', None)
        type_extract = count_data['message'].get('type', None)
        
        # Append the result to the DataFrame
        results_df = pd.concat([results_df, pd.DataFrame({'Title': [item['title']], 'ReferencedByCount': [referenced_by_count], 'PublicationType': [type_extract]})], ignore_index=True)

# Print or use the results DataFrame as needed
    #json.dump(results_df, f)
results_df.to_json('helpers/Citations_Pubtype.json',orient='records', lines=True)