from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    'hospital_pricing_data_loader',
    default_args=default_args,
    description='Load hospital pricing data from CSV files to MySQL',
    schedule_interval=timedelta(days=1),
    catchup=False
)
def hospital_pricing_data_loader_dag():
    @task.external_python(task_id="load_hospital_pricing_data", python='/home/airflow/dbvenv/bin/python')
    def load_data_to_database():
        import os
        import pandas as pd
        import logging
        from database import get_db
        from models import StagingTable  # Updated import
        from sqlalchemy.orm import Session
        from sqlalchemy.exc import SQLAlchemyError
        from datetime import datetime

        # Base directory for downloaded files
        BASE_DIR = '/opt/airflow/data/Hospital Price Transparency Data'

        REQUIRED_COLUMNS = {
            'Associated_Codes', 'Cash_Discount', 'DeIdentified_Max_Allowed',
            'Deidentified_Min_Allowed', 'description', 'Gross_Charge',
            'iobSelection', 'payer', 'Payer_Allowed_Amount'
        }

        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        logger = logging.getLogger(__name__)

        def process_csv_file(filepath, session):
            logger.info(f"Processing file: {filepath}")
            try:
                df = pd.read_csv(filepath, nrows=0)  # Read only the header

                if set(df.columns) != REQUIRED_COLUMNS:
                    logger.warning(f"Skipping file {filepath} - does not have the exact required columns")
                    logger.info(f"Columns in file: {df.columns}")
                    return

                df = pd.read_csv(filepath)  # Now read the full file

                parts = filepath.split(os.sep)
                state = parts[-5]
                city_name = parts[-4]
                zip_code = parts[-3]
                hospital_name = parts[-2]

                batch_size = 1000
                total_rows = len(df)
                for start in range(0, total_rows, batch_size):
                    end = min(start + batch_size, total_rows)
                    batch = df.iloc[start:end]

                    for _, row in batch.iterrows():
                        try:
                            staging_data = StagingTable(  # Updated to use StagingTable
                                cpt_code=row['Associated_Codes'],
                                cash_discount=None if pd.isna(row['Cash_Discount']) else row['Cash_Discount'],
                                deidentified_max_allowed=None if pd.isna(row['DeIdentified_Max_Allowed']) else row[
                                    'DeIdentified_Max_Allowed'],
                                deidentified_min_allowed=None if pd.isna(row['Deidentified_Min_Allowed']) else row[
                                    'Deidentified_Min_Allowed'],
                                description=row['description'],
                                gross_charge=None if pd.isna(row['Gross_Charge']) else row['Gross_Charge'],
                                payer_allowed_amount=None if pd.isna(row['Payer_Allowed_Amount']) else row[
                                    'Payer_Allowed_Amount'],
                                payer_id=str(row['payer']),
                                city_id=city_name,
                                zipcode_id=zip_code,
                                hospital_id=hospital_name
                            )
                            session.add(staging_data)

                        except Exception as e:
                            logger.error(f"Error processing row in {filepath}: {str(e)}")
                            continue

                    session.commit()
                    logger.info(f"Processed {end}/{total_rows} rows from {filepath}")

                logger.info(f"Completed processing file: {filepath}")

            except Exception as e:
                logger.error(f"Error processing file {filepath}: {str(e)}")
                session.rollback()

        db_generator = get_db()
        session = next(db_generator)

        start_time = datetime.now()
        total_files = 0
        processed_files = 0

        try:
            for root, dirs, files in os.walk(BASE_DIR):
                total_files += len([f for f in files if f.endswith('.csv')])

            for root, dirs, files in os.walk(BASE_DIR):
                for file in files:
                    if file.endswith('.csv'):
                        filepath = os.path.join(root, file)
                        try:
                            process_csv_file(filepath, session)
                            processed_files += 1
                            logger.info(f"Progress: {processed_files}/{total_files} files processed")
                        except SQLAlchemyError as e:
                            logger.error(f"Database error processing {filepath}: {str(e)}")
                            session.rollback()
                        except Exception as e:
                            logger.error(f"Unexpected error processing {filepath}: {str(e)}")
                            continue

        except Exception as e:
            logger.error(f"An error occurred during file processing: {str(e)}")
            raise
        finally:
            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(f"Total files processed: {processed_files}/{total_files}")
            logger.info(f"Total processing time: {duration}")
            session.close()

    load_data_to_database()

dag = hospital_pricing_data_loader_dag()


# from airflow.decorators import dag, task
# from airflow.utils.dates import days_ago
# from datetime import timedelta
#
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 3,
#     'retry_delay': timedelta(minutes=5),
# }
#
# @dag(
#     'hospital_pricing_data_loader',
#     default_args=default_args,
#     description='Load hospital pricing data from CSV files to MySQL',
#     schedule_interval=timedelta(days=1),
#     catchup=False
# )
# def hospital_pricing_data_loader_dag():
#     @task.external_python(task_id="load_hospital_pricing_data", python='/home/airflow/dbvenv/bin/python')
#     def load_data_to_database():
#         import os
#         import pandas as pd
#         import logging
#         from database import get_db
#         from models import HospitalPricingData
#         from sqlalchemy.orm import Session
#         from sqlalchemy.exc import SQLAlchemyError
#         from datetime import datetime
#
#         # Base directory for downloaded files
#         BASE_DIR = '/opt/airflow/data/Hospital Price Transparency Data'
#
#         REQUIRED_COLUMNS = {
#             'Associated_Codes', 'Cash_Discount', 'DeIdentified_Max_Allowed',
#             'Deidentified_Min_Allowed', 'description', 'Gross_Charge',
#             'iobSelection', 'payer', 'Payer_Allowed_Amount'
#         }
#
#         # Set up logging
#         logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#         logger = logging.getLogger(__name__)
#
#         def process_csv_file(filepath, session):
#             logger.info(f"Processing file: {filepath}")
#             try:
#                 df = pd.read_csv(filepath, nrows=0)  # Read only the header
#
#                 if set(df.columns) != REQUIRED_COLUMNS:
#                     logger.warning(f"Skipping file {filepath} - does not have the exact required columns")
#                     return
#
#                 df = pd.read_csv(filepath)  # Now read the full file
#
#                 parts = filepath.split(os.sep)
#                 state = parts[-5]
#                 city_name = parts[-4]
#                 zip_code = parts[-3]
#                 hospital_name = parts[-2]
#
#                 batch_size = 1000
#                 total_rows = len(df)
#                 for start in range(0, total_rows, batch_size):
#                     end = min(start + batch_size, total_rows)
#                     batch = df.iloc[start:end]
#
#                     for _, row in batch.iterrows():
#                         try:
#                             pricing_data = HospitalPricingData(
#                                 cpt_code=row['Associated_Codes'],
#                                 cash_discount=None if pd.isna(row['Cash_Discount']) else row['Cash_Discount'],
#                                 deidentified_max_allowed=None if pd.isna(row['DeIdentified_Max_Allowed']) else row[
#                                     'DeIdentified_Max_Allowed'],
#                                 deidentified_min_allowed=None if pd.isna(row['Deidentified_Min_Allowed']) else row[
#                                     'Deidentified_Min_Allowed'],
#                                 description=row['description'],
#                                 gross_charge=None if pd.isna(row['Gross_Charge']) else row['Gross_Charge'],
#                                 payer_allowed_amount=None if pd.isna(row['Payer_Allowed_Amount']) else row[
#                                     'Payer_Allowed_Amount'],
#                                 payer_id=str(row['payer']),
#                                 city_id=city_name,
#                                 zipcode_id=zip_code,
#                                 hospital_id=hospital_name
#                             )
#                             session.add(pricing_data)
#
#                         except Exception as e:
#                             logger.error(f"Error processing row in {filepath}: {str(e)}")
#                             continue
#
#                     session.commit()
#                     logger.info(f"Processed {end}/{total_rows} rows from {filepath}")
#
#                 logger.info(f"Completed processing file: {filepath}")
#
#             except Exception as e:
#                 logger.error(f"Error processing file {filepath}: {str(e)}")
#                 session.rollback()
#
#         db_generator = get_db()
#         session = next(db_generator)
#
#         start_time = datetime.now()
#         total_files = 0
#         processed_files = 0
#
#         try:
#             for root, dirs, files in os.walk(BASE_DIR):
#                 total_files += len([f for f in files if f.endswith('.csv')])
#
#             for root, dirs, files in os.walk(BASE_DIR):
#                 for file in files:
#                     if file.endswith('.csv'):
#                         filepath = os.path.join(root, file)
#                         try:
#                             process_csv_file(filepath, session)
#                             processed_files += 1
#                             logger.info(f"Progress: {processed_files}/{total_files} files processed")
#                         except SQLAlchemyError as e:
#                             logger.error(f"Database error processing {filepath}: {str(e)}")
#                             session.rollback()
#                         except Exception as e:
#                             logger.error(f"Unexpected error processing {filepath}: {str(e)}")
#                             continue
#
#         except Exception as e:
#             logger.error(f"An error occurred during file processing: {str(e)}")
#             raise
#         finally:
#             end_time = datetime.now()
#             duration = end_time - start_time
#             logger.info(f"Total files processed: {processed_files}/{total_files}")
#             logger.info(f"Total processing time: {duration}")
#             session.close()
#
#     load_data_to_database()
#
# dag = hospital_pricing_data_loader_dag()

# from airflow.decorators import dag, task
# from airflow.utils.dates import days_ago
# from datetime import timedelta
#
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 3,
#     'retry_delay': timedelta(minutes=5),
# }
#
# @dag(
#     'hospital_pricing_data_loader',
#     default_args=default_args,
#     description='Load hospital pricing data from CSV files to MySQL',
#     schedule_interval=timedelta(days=1),
#     catchup=False
# )
# def hospital_pricing_data_loader_dag():
#     @task.external_python(task_id="load_hospital_pricing_data", python='/home/airflow/dbvenv/bin/python')
#     def load_data_to_database():
#         import os
#         import pandas as pd
#         import logging
#         from database import get_db
#         from models import HospitalPricingData, Payer, City, ZipCode, Hospital, CptTranslation
#         from sqlalchemy.orm import Session
#         from sqlalchemy.exc import SQLAlchemyError
#         from datetime import datetime
#
#         # Base directory for downloaded files
#         BASE_DIR = '/opt/airflow/data/Hospital Price Transparency Data'
#
#         REQUIRED_COLUMNS = {
#             'Associated_Codes', 'Cash_Discount', 'DeIdentified_Max_Allowed',
#             'Deidentified_Min_Allowed', 'description', 'Gross_Charge',
#             'iobSelection', 'payer', 'Payer_Allowed_Amount'
#         }
#
#         # Set up logging
#         logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
#         logger = logging.getLogger(__name__)
#
#         def get_or_create(session, model, **kwargs):
#             instance = session.query(model).filter_by(**kwargs).first()
#             if instance:
#                 return instance, instance.id
#             else:
#                 instance = model(**kwargs)
#                 session.add(instance)
#                 session.flush()
#                 return instance, instance.id
#
#         def process_csv_file(filepath, session):
#             logger.info(f"Processing file: {filepath}")
#             try:
#                 df = pd.read_csv(filepath, nrows=0)  # Read only the header
#
#                 if set(df.columns) != REQUIRED_COLUMNS:
#                     logger.warning(f"Skipping file {filepath} - does not have the exact required columns")
#                     return
#
#                 df = pd.read_csv(filepath)  # Now read the full file
#
#                 parts = filepath.split(os.sep)
#                 state = parts[-5]
#                 city_name = parts[-4]
#                 zip_code = parts[-3]
#                 hospital_name = parts[-2]
#
#                 city, city_id = get_or_create(session, City, name=city_name)
#                 zipcode, zipcode_id = get_or_create(session, ZipCode, code=zip_code, city_id=city_id)
#                 hospital, hospital_id = get_or_create(session, Hospital, name=hospital_name, zipcode_id=zipcode_id)
#
#                 batch_size = 1000
#                 total_rows = len(df)
#                 for start in range(0, total_rows, batch_size):
#                     end = min(start + batch_size, total_rows)
#                     batch = df.iloc[start:end]
#
#                     for _, row in batch.iterrows():
#                         try:
#                             payer, payer_id = get_or_create(session, Payer, name=row['payer'])
#
#                             cpt_code = row['Associated_Codes'] if row['Associated_Codes'] != '' else None
#                             if cpt_code:
#                                 cpt_translation, _ = get_or_create(session, CptTranslation, cpt_code=cpt_code)
#
#                             pricing_data = HospitalPricingData(
#                                 cpt_code=cpt_code,
#                                 cash_discount=None if pd.isna(row['Cash_Discount']) else row['Cash_Discount'],
#                                 deidentified_max_allowed=None if pd.isna(row['DeIdentified_Max_Allowed']) else row['DeIdentified_Max_Allowed'],
#                                 deidentified_min_allowed=None if pd.isna(row['Deidentified_Min_Allowed']) else row['Deidentified_Min_Allowed'],
#                                 description=row['description'],
#                                 gross_charge=None if pd.isna(row['Gross_Charge']) else row['Gross_Charge'],
#                                 payer_allowed_amount=None if pd.isna(row['Payer_Allowed_Amount']) else row['Payer_Allowed_Amount'],
#                                 payer_id=payer_id,
#                                 city_id=city_id,
#                                 zipcode_id=zipcode_id,
#                                 hospital_id=hospital_id
#                             )
#                             session.add(pricing_data)
#
#                         except Exception as e:
#                             logger.error(f"Error processing row in {filepath}: {str(e)}")
#                             logger.error(f"Problematic row: {row}")
#                             continue
#
#                     session.commit()
#                     logger.info(f"Processed {end}/{total_rows} rows from {filepath}")
#
#                 logger.info(f"Completed processing file: {filepath}")
#
#             except Exception as e:
#                 logger.error(f"Error processing file {filepath}: {str(e)}")
#                 session.rollback()
#
#         db_generator = get_db()
#         session = next(db_generator)
#
#         start_time = datetime.now()
#         total_files = 0
#         processed_files = 0
#
#         try:
#             for root, dirs, files in os.walk(BASE_DIR):
#                 total_files += len([f for f in files if f.endswith('.csv')])
#
#             for root, dirs, files in os.walk(BASE_DIR):
#                 for file in files:
#                     if file.endswith('.csv'):
#                         filepath = os.path.join(root, file)
#                         try:
#                             process_csv_file(filepath, session)
#                             processed_files += 1
#                             logger.info(f"Progress: {processed_files}/{total_files} files processed")
#                         except SQLAlchemyError as e:
#                             logger.error(f"Database error processing {filepath}: {str(e)}")
#                             session.rollback()
#                         except Exception as e:
#                             logger.error(f"Unexpected error processing {filepath}: {str(e)}")
#                             continue
#
#         except Exception as e:
#             logger.error(f"An error occurred during file processing: {str(e)}")
#             raise
#         finally:
#             end_time = datetime.now()
#             duration = end_time - start_time
#             logger.info(f"Total files processed: {processed_files}/{total_files}")
#             logger.info(f"Total processing time: {duration}")
#             session.close()
#
#     load_data_to_database()
#
# dag = hospital_pricing_data_loader_dag()