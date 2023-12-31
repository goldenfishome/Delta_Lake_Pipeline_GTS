#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import os

def split_json(input_file, output_dir, split_size_bytes):
    '''
    function: split large json file into desired size
    input_file: path of input file, str 
    output_dir: name of output folder, str
    split_size_bytes: size of splitted file in MB, int
    '''
    current_size = 0
    current_data = []
    file_number = 1

    with open(input_file, 'r',encoding="cp437") as f:
        for line in f:
            item = json.loads(line)
            current_data.append(item)
            current_size += len(line)

            # If the current split size exceeds the specified limit, write to a new file
            if current_size >= split_size_bytes:
                output_file = os.path.join(output_dir, f'split_{file_number}.json')
                with open(output_file, 'w',encoding="utf8") as out_f:
                    for data_item in current_data:
                        json.dump(data_item, out_f)
                        out_f.write('\n')  # Add a newline character to separate objects
                current_data = []
                current_size = 0
                file_number += 1

    # Write any remaining data to a final split file
    if current_data:
        output_file = os.path.join(output_dir, f'split_{file_number}.json')
        with open(output_file, 'w',encoding="utf8") as out_f:
            for data_item in current_data:
                json.dump(data_item, out_f)
                out_f.write('\n')  # Add a newline character to separate objects
                
def main():
    # Define the input JSON file and the output directory for split files
    input_json_file = 'yelp_academic_dataset_user.json'
    output_directory = 'user_split_files'

    # Create the output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    # Set the desired split file size in bytes (e.g., 2GB)
    split_file_size_bytes = 1.5 * 1024 * 1024 * 1024

    # Split the large JSON file
    split_json(input_json_file, output_directory, split_file_size_bytes)


if __name__=="__main__":
    main()

