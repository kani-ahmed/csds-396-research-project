# from airflow import DAG
# from airflow.decorators import dag, task
# from datetime import datetime, timedelta
#
# # Define default arguments for the DAG
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
# @dag(
#     dag_id='load_cpt_translations',
#     default_args=default_args,
#     description='Load CPT translations from CSV to database',
#     schedule_interval=timedelta(days=1),
#     catchup=False
# )
# def load_cpt_translations_dag():
#     @task.external_python(task_id="load_cpt_translations", python='/home/airflow/dbvenv/bin/python')
#     def load_cpt_translations():
#         import logging
#         import pandas as pd
#         from sqlalchemy.exc import IntegrityError
#         import os
#         import sys
#         import numpy as np
#
#         # Add the project root to the Python path
#         sys.path.append('/opt/airflow')
#
#         logging.basicConfig(level=logging.INFO)
#         logger = logging.getLogger(__name__)
#
#         CSV_FILE_PATH = '/opt/airflow/data/data-cleaning-pipeline-generated-data/cpt-translation.csv'
#
#         try:
#             from extensions import db, create_app
#             from models import CptTranslation
#
#             app = create_app()
#             with app.app_context():
#                 # Check if CSV file exists
#                 if not os.path.exists(CSV_FILE_PATH):
#                     logger.error(f"CSV file not found: {CSV_FILE_PATH}")
#                     raise FileNotFoundError(f"CSV file not found: {CSV_FILE_PATH}")
#
#                 # Read CSV file
#                 df = pd.read_csv(CSV_FILE_PATH)
#                 logger.info(f"Read {len(df)} rows from CSV file")
#
#                 # Process each row
#                 for index, row in df.iterrows():
#                     cpt_code = row['CPT Code']
#                     description = row['Description']
#
#                     # Handle nan values
#                     if pd.isna(description) or description == 'nan':
#                         description = None  # or you could use an empty string: ''
#
#                     # Check if CPT code already exists
#                     existing_cpt = db.session.get(CptTranslation, cpt_code)
#                     if existing_cpt:
#                         # Update description if it has changed
#                         if existing_cpt.Description != description:
#                             existing_cpt.Description = description
#                             logger.info(f"Updated description for CPT code {cpt_code}")
#                     else:
#                         # Create new CptTranslation
#                         new_cpt = CptTranslation(CPT_Code=cpt_code, Description=description)
#                         db.session.add(new_cpt)
#                         logger.info(f"Added new CPT code: {cpt_code}")
#
#                     # Commit in batches to improve performance
#                     if index % 100 == 0:
#                         try:
#                             db.session.commit()
#                         except IntegrityError as e:
#                             db.session.rollback()
#                             logger.error(f"IntegrityError at index {index}: {str(e)}")
#
#                 # Final commit for any remaining records
#                 try:
#                     db.session.commit()
#                 except IntegrityError as e:
#                     db.session.rollback()
#                     logger.error(f"IntegrityError during final commit: {str(e)}")
#
#                 logger.info("CPT translations loaded successfully")
#
#         except Exception as e:
#             logger.error(f"An error occurred: {str(e)}")
#             raise
#
#     # Define the task
#     load_cpt_translations_task = load_cpt_translations()
#
# # Instantiate the DAG
# dag = load_cpt_translations_dag()