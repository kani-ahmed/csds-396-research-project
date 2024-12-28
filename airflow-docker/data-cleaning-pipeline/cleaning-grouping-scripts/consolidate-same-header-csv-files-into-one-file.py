import os
import csv
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_file(writer, file_path, city, zipcode, hospital):
    try:
        with open(file_path, "r", encoding='utf-8', errors='replace') as file:
            csv_reader = csv.DictReader(file)
            headers = csv_reader.fieldnames

            # Dynamically check if the column exists with a different name
            cash_discount_header = next((h for h in headers if "Cash_Discount" in h), None)
            deidentified_max_allowed_header = next((h for h in headers if "Deidentified_Max_Allowed" in h), None)

            for row in csv_reader:
                # Check if 'Associated_Codes' contains a CPT code
                code = row.get('Associated_Codes', '').strip()
                if code.isdigit() and len(code) == 5:
                    # Write the relevant data to the output file, along with the city, zipcode, and hospital
                    writer.writerow([
                        row['Associated_Codes'],
                        row.get(cash_discount_header, 'N/A'),
                        row.get(deidentified_max_allowed_header, 'N/A'),
                        row['Deidentified_Min_Allowed'],
                        row['payer'], row['iobSelection'],
                        city, zipcode, hospital])
            return True
    except Exception as e:
        logging.error(f"Failed to process file {file_path}: {e}")
        return False

def extract_rows_and_add_columns(directory):
    # Output file setup
    output_file = Path("/opt/airflow/data/all-rows-with-only-CPT-all-csv-files-combined.csv")
    # Ensure the output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Open the output file
    with open(output_file, "w", newline='') as out_csv:
        writer = csv.writer(out_csv)
        # Write the header for the output CSV file
        writer.writerow([
            'Associated_Codes', 'Cash_Discount', 'Deidentified_Max_Allowed',
            'Deidentified_Min_Allowed', 'payer', 'iobSelection',
            'City', 'ZipCode', 'Hospital'
        ])

        def process_directory(file_path, city, zipcode, hospital):
            logging.info(f"Processing file: {file_path} in {city}, {zipcode}, {hospital}")
            if process_file(writer, file_path, city, zipcode, hospital):
                logging.info(f"Successfully processed file: {file_path.name}")
            else:
                logging.warning(f"Skipped file due to errors: {file_path.name}")

        with ThreadPoolExecutor() as executor:
            futures = []
            for root, dirs, files in os.walk(directory):
                for filename in files:
                    if filename.endswith(".csv"):
                        file_path = Path(root) / filename
                        # Skip the output file if found
                        if file_path == output_file:
                            continue
                        # Extract city, zipcode, and hospital from the directory path
                        parts = root.split(os.sep)
                        if len(parts) < 3:
                            logging.warning(f"Skipping file {file_path} due to insufficient directory structure")
                            continue
                        city = parts[-3]
                        zipcode = parts[-2]
                        hospital = parts[-1]
                        futures.append(executor.submit(process_directory, file_path, city, zipcode, hospital))

            for future in as_completed(futures):
                future.result()  # Wait for all futures to complete

    logging.info(f"Extraction and consolidation complete. See '{output_file}' for results.")

def main():
    # Set the path to the root directory containing the .csv files
    root_directory = "/opt/airflow/data/all-csv-files-with-common-headers"

    # Extract rows containing CPT codes, add 'City', 'ZipCode', and 'Hospital', and save to
    # 'all-rows-with-only-CPT-all-csv-files-combined.csv'
    extract_rows_and_add_columns(root_directory)

if __name__ == "__main__":
    main()





# import os
# import csv
# import logging
#
# # Configure logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#
#
# def process_file(writer, file_path, city, zipcode, hospital):
#     try:
#         with open(file_path, "r") as file:
#             csv_reader = csv.DictReader(file)
#             headers = csv_reader.fieldnames
#
#             # Dynamically check if the column exists with a different name
#             cash_discount_header = next((h for h in headers if "Cash_Discount" in h), None)
#             deidentified_max_allowed_header = next((h for h in headers if "Deidentified_Max_Allowed" in h), None)
#
#             for row in csv_reader:
#                 # Check if 'Associated_Codes' contains a CPT code
#                 code = row.get('Associated_Codes', '').strip()
#                 if code.isdigit() and len(code) == 5:
#                     # Write the relevant data to the output file, along with the city, zipcode, and hospital
#                     writer.writerow([
#                         row['Associated_Codes'],
#                         row.get(cash_discount_header, 'N/A'),
#                         row.get(deidentified_max_allowed_header, 'N/A'),
#                         row['Deidentified_Min_Allowed'],
#                         row['payer'], row['iobSelection'],
#                         city, zipcode, hospital])
#             return True
#     except Exception as e:
#         logging.error(f"Failed to process file {file_path}: {e}")
#         return False
#
#
# def extract_rows_and_add_columns(directory):
#     # Output file setup
#     output_file = os.path.join(os.getcwd(), "all-rows-with-only-CPT-all-csv-files-combined.csv")
#     # Ensure the output directory exists
#     os.makedirs(os.path.dirname(output_file), exist_ok=True)
#
#     # Open the output file
#     with open(output_file, "w", newline='') as out_csv:
#         writer = csv.writer(out_csv)
#         # Write the header for the output CSV file
#         writer.writerow([
#             'Associated_Codes', 'Cash_Discount', 'Deidentified_Max_Allowed',
#             'Deidentified_Min_Allowed', 'payer', 'iobSelection',
#             'City', 'ZipCode', 'Hospital'
#         ])
#
#         for root, dirs, files in os.walk(directory):
#             for filename in files:
#                 if filename.endswith(".csv"):
#                     file_path = os.path.join(root, filename)
#                     # Extract city, zipcode, and hospital from the directory path
#                     parts = root.split(os.sep)
#                     city = parts[-3]
#                     zipcode = parts[-2]
#                     hospital = parts[-1]
#
#                     logging.info(f"Processing file: {file_path} in {city}, {zipcode}, {hospital}")
#
#                     if process_file(writer, file_path, city, zipcode, hospital):
#                         logging.info(f"Successfully processed file: {filename}")
#                     else:
#                         logging.warning(f"Skipped file due to errors: {filename}")
#
#     logging.info(f"Extraction and consolidation complete. See 'all-rows-with-only-CPT-all-csv-files-combined.csv' for "
#                  f"results.")
#
#
# def main():
#     # Set the path to the root directory containing the .csv files
#     root_directory = "/Users/kani/PycharmProjects/Hospital_Price_Transparency_Project/all-csv-files-with-common-headers"
#
#     # Extract rows containing CPT codes, add 'City', 'ZipCode', and 'Hospital', and save to
#     # 'all-rows-with-only-CPT-all-csv-files-combined.csv'
#     extract_rows_and_add_columns(root_directory)
#
#
# if __name__ == "__main__":
#     main()
