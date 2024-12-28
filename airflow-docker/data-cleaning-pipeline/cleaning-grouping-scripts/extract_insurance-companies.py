import os
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

def extract_payer_values_from_file(file_path):
    payer_values = set()

    try:
        with open(file_path, "r") as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)  # Read the header row

            # Find the index of the "payer" column
            payer_index = -1
            for i, column in enumerate(header):
                if column.lower() == "payer":
                    payer_index = i
                    break

            # If the "payer" column is found, extract the values
            if payer_index != -1:
                for row in csv_reader:
                    if len(row) > payer_index:
                        payer_value = row[payer_index].strip()
                        if payer_value:
                            payer_values.add(payer_value)
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

    return payer_values

def extract_payer_values(directory, output_directory):
    print("Extracting payer values...")
    all_payer_values = set()

    files_to_process = []
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith(".csv"):
                file_path = os.path.join(root, filename)
                files_to_process.append(file_path)

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(extract_payer_values_from_file, file_path) for file_path in files_to_process]
        for future in as_completed(futures):
            payer_values = future.result()
            all_payer_values.update(payer_values)

    # Write the payer values to a file in the specified output directory
    os.makedirs(output_directory, exist_ok=True)
    payer_values_file = os.path.join(output_directory, "payer_values.txt")
    with open(payer_values_file, "w") as file:
        file.write("\n".join(all_payer_values))
    print(f"Payer values saved to: {payer_values_file}")

    print("Extraction complete.")
    print(f"Total unique payer values found: {len(all_payer_values)}")

def main():
    # Set the path to the root directory containing the .csv files
    root_directory = "/opt/airflow/data/all-csv-files-with-common-headers"
    # Set the path to the output directory
    output_directory = "/opt/airflow/data"

    # Extract payer values from the .csv files in the root directory and its subdirectories
    extract_payer_values(root_directory, output_directory)

if __name__ == "__main__":
    main()


# import os
# import csv
#
#
# def extract_payer_values(directory):
#     print("Extracting payer values...")
#     payer_values = set()
#
#     # Iterate through each file and subdirectory in the directory
#     for root, dirs, files in os.walk(directory):
#         for filename in files:
#             if filename.endswith(".csv"):
#                 file_path = os.path.join(root, filename)
#                 print(f"Processing file: {file_path}")
#
#                 # Open the .csv file and read the rows
#                 with open(file_path, "r") as file:
#                     csv_reader = csv.reader(file)
#                     header = next(csv_reader)  # Read the header row
#
#                     # Find the index of the "payer" column
#                     payer_index = -1
#                     for i, column in enumerate(header):
#                         if column.lower() == "payer":
#                             payer_index = i
#                             break
#
#                     # If the "payer" column is found, extract the values
#                     if payer_index != -1:
#                         for row in csv_reader:
#                             if len(row) > payer_index:
#                                 payer_value = row[payer_index].strip()
#                                 if payer_value:
#                                     payer_values.add(payer_value)
#                                     print(f"Payer value found: {payer_value}")
#                     else:
#                         print(f"Payer column not found in file: {filename}")
#
#     # Write the payer values to a file in the current directory
#     current_dir = os.getcwd()
#     payer_values_file = os.path.join(current_dir, "payer_values.txt")
#     with open(payer_values_file, "w") as file:
#         file.write("\n".join(payer_values))
#     print(f"Payer values saved to: {payer_values_file}")
#
#     print("Extraction complete.")
#     print(f"Total unique payer values found: {len(payer_values)}")
#
#
# def main():
#     # Set the path to the root directory containing the .csv files
#     root_directory = "/Users/kani/PycharmProjects/Hospital_Price_Transparency_Project/all-csv-files-with-common-headers"
#
#     # Extract payer values from the .csv files in the root directory and its subdirectories
#     extract_payer_values(root_directory)
#
#
# if __name__ == "__main__":
#     main()