import json

def count_entries_in_json(file_path):
    count = 0
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            try:
                # Assuming each line is a separate JSON object
                entry = json.loads(line)
                if 'id' in entry:
                    count += 1
            except json.JSONDecodeError:
                # Handle possible decoding errors (e.g., empty lines)
                continue
    return count

file_path = 'arxiv-metadata-oai-snapshot.json'
total_entries = count_entries_in_json(file_path)
print(f'Total number of entries: {total_entries}')
