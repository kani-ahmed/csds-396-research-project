# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# import os
# import logging
# import csv
# from flask import Flask
# from extensions import db
# from models.city import City
# from models.zipcode import ZipCode
# from models.hospital import Hospital
# from models.payer import Payer
# from models.cpt_translation import CptTranslation
# from models.hospital_charge import HospitalCharge
# from models.icd_cpt_mapping import IcdCptMapping
#
# # Configure logging
# logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
# logger = logging.getLogger(__name__)
#
# # Create a Flask app and configure SQLAlchemy with MySQL
# app = Flask(__name__)
# app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://airflow:airflow@mysql/airflow'
# app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# db.init_app(app)
#
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 7, 24),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }
#
# dag = DAG(
#     'hospital_price_transparency',
#     default_args=default_args,
#     description='A DAG to load Hospital Price Transparency data',
#     schedule_interval=timedelta(days=1),
# )
#
# def get_or_create(session, model, **kwargs):
#     try:
#         return session.query(model).filter_by(**kwargs).one()
#     except NoResultFound:
#         instance = model(**kwargs)
#         session.add(instance)
#         session.commit()
#         return instance
#
# def load_city_zipcode_hospital_payer():
#     base_dir = '/opt/airflow/data/Hospital_Price_Transparency_Data'
#     with app.app_context():
#         for state_dir in os.listdir(base_dir):
#             state_path = os.path.join(base_dir, state_dir)
#             if os.path.isdir(state_path):
#                 for city_dir in os.listdir(state_path):
#                     city_path = os.path.join(state_path, city_dir)
#                     if os.path.isdir(city_path):
#                         for zipcode_dir in os.listdir(city_path):
#                             zipcode_path = os.path.join(city_path, zipcode_dir)
#                             if os.path.isdir(zipcode_path):
#                                 for csv_file in os.listdir(zipcode_path):
#                                     csv_file_path = os.path.join(zipcode_path, csv_file)
#                                     with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
#                                         reader = csv.DictReader(csvfile)
#                                         for row in reader:
#                                             city_name = row['City']
#                                             zip_code = row['ZipCode']
#                                             hospital_name = row['Hospital']
#                                             payer_name = row['payer']
#
#                                             # Insert City
#                                             city = get_or_create(db.session, City, name=city_name)
#
#                                             # Insert ZipCode and link to City
#                                             zipcode = get_or_create(db.session, ZipCode, code=zip_code, city_id=city.id)
#
#                                             # Insert Hospital and link to ZipCode
#                                             hospital = get_or_create(db.session, Hospital, name=hospital_name, zipcode_id=zipcode.id)
#
#                                             # Insert Payer and ensure linkage to ZipCode
#                                             payer = get_or_create(db.session, Payer, name=payer_name)
#
#                                             # Link Payer to ZipCode if not already linked
#                                             if zipcode not in payer.zipcodes:
#                                                 payer.zipcodes.append(zipcode)
#                                                 db.session.commit()
#
# def load_cpt_translation():
#     csv_file_path = '/opt/airflow/data/Hospital_Price_Transparency_Data/cpt-translation.csv'
#     with app.app_context():
#         with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
#             reader = csv.DictReader(csvfile)
#             entries = []
#             for row in reader:
#                 cpt_code = row['CPT Code']
#                 if not CptTranslation.query.get(cpt_code):
#                     db_entry = CptTranslation(CPT_Code=cpt_code, Description=row['Description'])
#                     entries.append(db_entry)
#
#             logger.debug(f"Loaded {len(entries)} entries from {csv_file_path}")
#             db.session.add_all(entries)
#             db.session.commit()
#             logger.info(f"Successfully committed {len(entries)} entries to the database")
#
# def get_foreign_key_id(session, model, **kwargs):
#     try:
#         return session.query(model).filter_by(**kwargs).one().id
#     except NoResultFound:
#         return None
#
# def load_hospital_charge():
#     base_dir = '/opt/airflow/data/Hospital_Price_Transparency_Data'
#     with app.app_context():
#         for state_dir in os.listdir(base_dir):
#             state_path = os.path.join(base_dir, state_dir)
#             if os.path.isdir(state_path):
#                 for city_dir in os.listdir(state_path):
#                     city_path = os.path.join(state_path, city_dir)
#                     if os.path.isdir(city_path):
#                         for zipcode_dir in os.listdir(city_path):
#                             zipcode_path = os.path.join(city_path, zipcode_dir)
#                             if os.path.isdir(zipcode_path):
#                                 for csv_file in os.listdir(zipcode_path):
#                                     csv_file_path = os.path.join(zipcode_path, csv_file)
#                                     with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
#                                         reader = csv.DictReader(csvfile)
#                                         entries = []
#                                         for row in reader:
#                                             associated_codes = row['Associated_Codes']
#                                             if CptTranslation.query.get(associated_codes):
#                                                 cash_discount = row['Cash_Discount']
#                                                 if cash_discount == 'N/A':
#                                                     cash_discount = None  # Set 'N/A' to None for Numeric columns
#
#                                                 deidentified_min_allowed = row['Deidentified_Min_Allowed']
#                                                 if deidentified_min_allowed == 'N/A':
#                                                     deidentified_min_allowed = None  # Set 'N/A' to None for Numeric columns
#
#                                                 # Fetch foreign key ids
#                                                 payer_id = get_foreign_key_id(db.session, Payer, name=row['payer'])
#                                                 city_id = get_foreign_key_id(db.session, City, name=row['City'])
#                                                 zipcode_id = get_foreign_key_id(db.session, ZipCode, code=row['ZipCode'])
#                                                 hospital_id = get_foreign_key_id(db.session, Hospital, name=row['Hospital'])
#
#                                                 if not all([payer_id, city_id, zipcode_id, hospital_id]):
#                                                     logger.warning(f"Skipping row due to missing foreign keys: {row}")
#                                                     continue
#
#                                                 db_entry = HospitalCharge(
#                                                     Associated_Codes=associated_codes,
#                                                     Cash_Discount=cash_discount,
#                                                     Deidentified_Max_Allowed=row.get('Deidentified_Max_Allowed', 'N/A'),
#                                                     Deidentified_Min_Allowed=deidentified_min_allowed,
#                                                     payer_id=payer_id,
#                                                     city_id=city_id,
#                                                     zipcode_id=zipcode_id,
#                                                     hospital_id=hospital_id
#                                                 )
#                                                 entries.append(db_entry)
#                                             else:
#                                                 logger.warning(f"Skipping row with Associated_Codes '{associated_codes}' as it does not exist in CptTranslations")
#
#                                         logger.debug(f"Loaded {len(entries)} entries from {csv_file_path}")
#                                         db.session.add_all(entries)
#                                         db.session.commit()
#                                         logger.info(f"Successfully committed {len(entries)} entries to the database")
#
# def load_icd_cpt_mappings():
#     code_mappings_directory = '/opt/airflow/data/COLLECTED-CPT-ICD10-CM-CODE_MAPPINGS'
#     with app.app_context():
#         # Get all CPT codes from the CptTranslation table and store in a set for quick lookup
#         existing_cpt_codes = {cpt.CPT_Code for cpt in CptTranslation.query.all()}
#
#         files = os.listdir(code_mappings_directory)
#         for file in files:
#             if file.endswith('_code_mappings.csv'):
#                 cpt_code = file.split('_')[0]
#                 if cpt_code not in existing_cpt_codes:
#                     logger.warning(
#                         f"CPT code {cpt_code} from file {file} does not exist in CptTranslations table. Skipping file.")
#                     continue
#                 csv_file_path = os.path.join(code_mappings_directory, file)
#                 with open(csv_file_path, newline='', encoding='utf-8') as csvfile:
#                     reader = csv.DictReader(csvfile)
#                     entries = []
#                     for row in reader:
#                         icd_code = row['Diagnosis Code']
#                         db_entry = IcdCptMapping(ICD_Code=icd_code, CPT_Code=cpt_code)
#                         entries.append(db_entry)
#                     logger.debug(f"Loaded {len(entries)} entries from {file}")
#                     db.session.add_all(entries)
#                     try:
#                         db.session.commit()
#                         logger.info(f"Successfully committed ICD-CPT mappings from {file}")
#                     except Exception as e:
#                         db.session.rollback()
#                         logger.error(f"An error occurred while inserting data from {file}: {e}")
#
# # Define Airflow tasks
# t1 = PythonOperator(
#     task_id='load_city_zipcode_hospital_payer',
#     python_callable=load_city_zipcode_hospital_payer,
#     dag=dag,
# )
#
# t2 = PythonOperator(
#     task_id='load_cpt_translation',
#     python_callable=load_cpt_translation,
#     dag=dag,
# )
#
# t3 = PythonOperator(
#     task_id='load_hospital_charge',
#     python_callable=load_hospital_charge,
#     dag=dag,
# )
#
# t4 = PythonOperator(
#     task_id='load_icd_cpt_mappings',
#     python_callable=load_icd_cpt_mappings,
#     dag=dag,
# )
#
# # Define task dependencies
# t1 >> t2 >> t3 >> t4
