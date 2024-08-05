# from airflow import DAG
# from airflow.decorators import dag, task
# from datetime import datetime, timedelta
# import pandas as pd
# import os
# import logging
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.exc import IntegrityError, OperationalError
# from decimal import Decimal
#
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 7, 24),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 3,
#     'retry_delay': timedelta(minutes=5),
# }
#
#
# @dag(
#     dag_id='load_hospital_pricing_data',
#     default_args=default_args,
#     description='Load Hospital Pricing Data from CSV files',
#     schedule_interval=timedelta(days=1),
#     catchup=False
# )
# def load_hospital_pricing_data_dag():
#     @task.external_python(task_id="process_csv_files", python='/home/airflow/dbvenv/bin/python')
#     def process_csv_files():
#         import logging
#         import os
#         import sys
#         import pandas as pd
#         from sqlalchemy.orm import sessionmaker
#         from sqlalchemy.exc import IntegrityError, OperationalError
#         from decimal import Decimal
#
#         sys.path.append('/opt/airflow')
#
#         logging.basicConfig(level=logging.INFO)
#         logger = logging.getLogger(__name__)
#
#         BASE_DIR = '/opt/airflow/data/Hospital Price Transparency Data/OH'
#
#         try:
#             from extensions import db, create_app
#             from models import City, ZipCode, Hospital, Payer, CptTranslation, HospitalPricingData
#
#             app = create_app()
#             with app.app_context():
#                 Session = sessionmaker(bind=db.engine)
#                 session = Session()
#
#                 for root, dirs, files in os.walk(BASE_DIR):
#                     for file in files:
#                         if file.endswith('.csv'):
#                             file_path = os.path.join(root, file)
#                             logger.info(f"Processing file: {file_path}")
#                             try:
#                                 df = pd.read_csv(file_path, encoding='utf-8', encoding_errors='replace',
#                                                  low_memory=False)
#
#                                 required_columns = ['Associated_Codes', 'Cash_Discount', 'DeIdentified_Max_Allowed',
#                                                     'Deidentified_Min_Allowed', 'payer']
#                                 if not all(col in df.columns for col in required_columns):
#                                     logger.warning(f"Skipping file {file_path} - missing required columns")
#                                     continue
#
#                                 # Extract city, zipcode, and hospital from file path
#                                 path_parts = file_path.split(os.sep)
#                                 city_name = path_parts[-4]
#                                 zipcode = path_parts[-3]
#                                 hospital_name = path_parts[-2].replace('_', ' ')
#
#                                 city = session.query(City).filter_by(name=city_name).first()
#                                 zipcode_obj = session.query(ZipCode).filter_by(code=zipcode).first()
#                                 hospital = session.query(Hospital).filter_by(name=hospital_name).first()
#
#                                 if not all([city, zipcode_obj, hospital]):
#                                     logger.warning(
#                                         f"Skipping file {file_path} - missing city, zipcode, or hospital data")
#                                     continue
#
#                                 entries = []
#                                 for _, row in df.iterrows():
#                                     payer = session.query(Payer).filter_by(name=row['payer']).first()
#                                     if not payer:
#                                         payer = Payer(name=row['payer'])
#                                         session.add(payer)
#                                         session.flush()
#
#                                     cpt = session.query(CptTranslation).filter_by(
#                                         CPT_Code=row['Associated_Codes']).first()
#                                     if not cpt:
#                                         logger.warning(f"Skipping row - CPT code {row['Associated_Codes']} not found")
#                                         continue
#
#                                     pricing_data = HospitalPricingData(
#                                         cpt_code=row['Associated_Codes'],
#                                         Cash_Discount=Decimal(str(row['Cash_Discount'])) if pd.notna(
#                                             row['Cash_Discount']) else None,
#                                         Deidentified_Max_Allowed=str(row['DeIdentified_Max_Allowed']) if pd.notna(
#                                             row['DeIdentified_Max_Allowed']) else None,
#                                         Deidentified_Min_Allowed=Decimal(
#                                             str(row['Deidentified_Min_Allowed'])) if pd.notna(
#                                             row['Deidentified_Min_Allowed']) else None,
#                                         payer_id=payer.id,
#                                         city_id=city.id,
#                                         zipcode_id=zipcode_obj.id,
#                                         hospital_id=hospital.id
#                                     )
#                                     entries.append(pricing_data)
#
#                                     if len(entries) >= 1000:
#                                         session.bulk_save_objects(entries)
#                                         session.commit()
#                                         entries = []
#
#                                 if entries:
#                                     session.bulk_save_objects(entries)
#                                     session.commit()
#
#                                 logger.info(f"Successfully processed and loaded: {file_path}")
#
#                             except Exception as e:
#                                 session.rollback()
#                                 logger.error(f"Failed to process {file_path}: {str(e)}")
#
#                 session.close()
#                 logger.info("All CSV files processed")
#
#         except Exception as e:
#             logger.error(f"An error occurred: {str(e)}")
#             raise
#
#     process_csv_files_task = process_csv_files()
#
#
# dag = load_hospital_pricing_data_dag()