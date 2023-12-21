import json

def split_json_file(file_path, chunk_size, total_chunks):
    chunk_count = 0

    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            for chunk_index in range(total_chunks):
                chunk = []
                for _ in range(chunk_size):
                    line = file.readline()
                    if not line:
                        break  # End of file
                    chunk.append(json.loads(line))

                if chunk:
                    with open(f'chunk_{chunk_index+1}.json', 'w', encoding='utf-8') as output_file:
                        json.dump(chunk, output_file, indent=4)

                    chunk_count += 1
                    print(f'Chunk {chunk_index+1} created.')
                else:
                    break  # No more data to read

        print(f'{chunk_count} chunks were created.')
    except Exception as e:
        print(f"An error occurred: {e}")

file_path = 'arxiv-metadata-oai-snapshot.json'
split_json_file(file_path, 20000, 10)
