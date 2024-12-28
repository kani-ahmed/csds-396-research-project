import os
import json
import csv
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

def process_json_file(file_path, parent_dir, relevant_json_dir):
    try:
        print(f"Processing JSON file: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if '"Associated_Codes"' in line:
                    destination_dir = os.path.join(relevant_json_dir, os.path.relpath(os.path.dirname(file_path), parent_dir))
                    os.makedirs(destination_dir, exist_ok=True)
                    shutil.copy2(file_path, destination_dir)
                    return 1
    except Exception as e:
        print(f"Error processing JSON file {file_path}: {e}")
    return 0

def process_csv_file(file_path, parent_dir, relevant_csv_dir):
    try:
        print(f"Processing CSV file: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            csv_reader = csv.reader(f)
            header = next(csv_reader)
            if "Associated_Codes" in header:
                destination_dir = os.path.join(relevant_csv_dir, os.path.relpath(os.path.dirname(file_path), parent_dir))
                os.makedirs(destination_dir, exist_ok=True)
                shutil.copy2(file_path, destination_dir)
                return 1
    except Exception as e:
        print(f"Error processing CSV file {file_path}: {e}")
    return 0

def search_and_copy_associated_codes_files(parent_dir, relevant_json_dir, relevant_csv_dir):
    json_count = 0
    csv_count = 0
    json_associated_codes_count = 0
    csv_associated_codes_count = 0

    files_to_process = []
    for root, dirs, files in os.walk(parent_dir):
        for file in files:
            file_path = os.path.join(root, file)
            if file.endswith('.json'):
                json_count += 1
                files_to_process.append((process_json_file, file_path))
            elif file.endswith('.csv'):
                csv_count += 1
                files_to_process.append((process_csv_file, file_path))

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(func, file_path, parent_dir, relevant_json_dir if func == process_json_file else relevant_csv_dir) for func, file_path in files_to_process]
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    if future.result() == 1:
                        if future.func == process_json_file:
                            json_associated_codes_count += 1
                        elif future.func == process_csv_file:
                            csv_associated_codes_count += 1
            except Exception as e:
                print(f"Error in future result: {e}")

    print(f"Total number of JSON files: {json_count}")
    print(f"Total number of CSV files: {csv_count}")
    print(f"Total JSON files containing 'Associated_Codes': {json_associated_codes_count}")
    print(f"Total CSV files containing 'Associated_Codes': {csv_associated_codes_count}")

# Replace the below strings with your actual directory paths
parent_dir = "/opt/airflow/dags/Hospital Price Transparency Data/OH"
relevant_json_dir = "/opt/airflow/data/all-json-files-with-common-headers"
relevant_csv_dir = "/opt/airflow/data/all-csv-files-with-common-headers"

# Create the all-json-files-with-common-headers and all-csv-files-with-common-headers directories if they don't exist
os.makedirs(relevant_json_dir, exist_ok=True)
os.makedirs(relevant_csv_dir, exist_ok=True)

search_and_copy_associated_codes_files(parent_dir, relevant_json_dir, relevant_csv_dir)



# import os
# import json
# import csv
# import shutil
#
#
# def search_and_copy_associated_codes_files(parent_dir, relevant_json_dir, relevant_csv_dir):
#     json_count = 0
#     csv_count = 0
#     json_associated_codes_count = 0
#     csv_associated_codes_count = 0
#
#     for root, dirs, files in os.walk(parent_dir):
#         for file in files:
#             file_path = os.path.join(root, file)
#             if file.endswith('.json'):
#                 json_count += 1
#                 try:
#                     with open(file_path, 'r', encoding='utf-8') as f:
#                         data = json.load(f)
#                         data_str = json.dumps(data)
#                         if "Associated_Codes" in data_str:
#                             json_associated_codes_count += 1
#                             destination_dir = os.path.join(relevant_json_dir, os.path.relpath(root, parent_dir))
#                             os.makedirs(destination_dir, exist_ok=True)
#                             shutil.copy2(file_path, destination_dir)
#                 except Exception as e:
#                     print(f"Error processing file {file_path}: {e}")
#             elif file.endswith('.csv'):
#                 csv_count += 1
#                 try:
#                     with open(file_path, 'r', encoding='utf-8') as f:
#                         csv_reader = csv.reader(f)
#                         header = next(csv_reader)
#                         if "Associated_Codes" in header:
#                             csv_associated_codes_count += 1
#                             destination_dir = os.path.join(relevant_csv_dir, os.path.relpath(root, parent_dir))
#                             os.makedirs(destination_dir, exist_ok=True)
#                             shutil.copy2(file_path, destination_dir)
#                 except Exception as e:
#                     print(f"Error processing file {file_path}: {e}")
#
#     print(f"Total number of JSON files: {json_count}")
#     print(f"Total number of CSV files: {csv_count}")
#     print(f"Total JSON files containing 'Associated_Codes': {json_associated_codes_count}")
#     print(f"Total CSV files containing 'Associated_Codes': {csv_associated_codes_count}")
#
#
# # Replace the below strings with your actual directory paths
# parent_dir = "/opt/airflow/dags/Hospital Price Transparency Data/OH"
# relevant_json_dir = "/opt/airflow/dags/all-json-files-with-common-headers"
# relevant_csv_dir = "/opt/airflow/dags/all-csv-files-with-common-headers"
#
# # Create the all-json-files-with-common-headers and all-csv-files-with-common-headers directories if they don't exist
# os.makedirs(relevant_json_dir, exist_ok=True)
# os.makedirs(relevant_csv_dir, exist_ok=True)
#
# search_and_copy_associated_codes_files(parent_dir, relevant_json_dir, relevant_csv_dir)