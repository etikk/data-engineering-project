import os
import json
import pandas as pd

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
                            if len(results) >= 20:
                                return results
    return results

# Usage:
#cat = 'categories'
#titl = 'title'
#print(extract_values(cat, titl))