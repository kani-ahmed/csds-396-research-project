# from airflow import DAG
# from airflow.decorators import dag, task
# from datetime import datetime, timedelta
# import logging
# import sys
# import os
#
# # Add the project root to the Python path
# sys.path.append('/opt/airflow')
#
# # Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
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
#     dag_id='load_location_data',
#     default_args=default_args,
#     description='Load location data from directory structure',
#     schedule_interval=timedelta(days=1),
#     catchup=False
# )
# def load_location_data_dag():
#     @task.external_python(task_id="load_location_data", python='/home/airflow/dbvenv/bin/python')
#     def load_location_data():
#         import logging
#         import os
#         import sys
#         import sqlalchemy
#
#         logging.basicConfig(level=logging.INFO)
#         logger = logging.getLogger(__name__)
#
#         # Add the project root to the Python path
#         sys.path.append('/opt/airflow')
#
#         # Define BASE_DIR within the function
#         BASE_DIR = '/opt/airflow/data/Hospital Price Transparency Data/OH'
#
#         try:
#             from extensions import db, create_app
#             from models import City, ZipCode, Hospital
#
#             def get_or_create(model, **kwargs):
#                 """
#                 Get an existing instance from the database or create a new one.
#                 Handles potential IntegrityErrors due to race conditions or duplicates.
#                 """
#                 instance = db.session.query(model).filter_by(**kwargs).first()
#                 if instance:
#                     return instance, False
#                 else:
#                     instance = model(**kwargs)
#                     try:
#                         db.session.add(instance)
#                         db.session.commit()
#                         return instance, True
#                     except sqlalchemy.exc.IntegrityError as e:
#                         db.session.rollback()
#                         if "Duplicate entry" in str(e) and model == ZipCode:
#                             # If it's a duplicate ZIP code, we need to handle it differently
#                             existing_zipcode = db.session.query(ZipCode).filter_by(code=kwargs['code']).first()
#                             if existing_zipcode:
#                                 logger.warning(f"ZIP code {kwargs['code']} already exists for a different city. Using existing ZIP code.")
#                                 return existing_zipcode, False
#                         else:
#                             # For other integrity errors, or if it's not a ZipCode, re-raise the exception
#                             raise
#
#                 # If we got here, something went wrong
#                 logger.error(f"Failed to create or retrieve {model.__name__}: {kwargs}")
#                 raise Exception(f"Failed to create or retrieve {model.__name__}")
#
#             app = create_app()
#             with app.app_context():
#                 # Check if BASE_DIR exists
#                 if not os.path.exists(BASE_DIR):
#                     logger.error(f"Base directory {BASE_DIR} does not exist.")
#                     raise FileNotFoundError(f"Base directory {BASE_DIR} does not exist.")
#
#                 # Process cities, zipcodes, and hospitals
#                 for city_name in os.listdir(BASE_DIR):
#                     city_path = os.path.join(BASE_DIR, city_name)
#                     if os.path.isdir(city_path):
#                         logger.info(f"Processing city: {city_name}")
#                         city, city_created = get_or_create(City, name=city_name)
#                         if city_created:
#                             logger.info(f"Created new city: {city_name}")
#                         else:
#                             logger.info(f"Retrieved existing city: {city_name}")
#
#                         for zipcode in os.listdir(city_path):
#                             zipcode_path = os.path.join(city_path, zipcode)
#                             if os.path.isdir(zipcode_path):
#                                 logger.info(f"Processing zipcode: {zipcode} in {city_name}")
#                                 zipcode_obj, zipcode_created = get_or_create(ZipCode, code=zipcode, city_id=city.id)
#                                 if zipcode_created:
#                                     logger.info(f"Created new zipcode: {zipcode}")
#                                 else:
#                                     logger.info(f"Retrieved existing zipcode: {zipcode}")
#
#                                 for hospital_name in os.listdir(zipcode_path):
#                                     hospital_path = os.path.join(zipcode_path, hospital_name)
#                                     if os.path.isdir(hospital_path):
#                                         hospital_name = hospital_name.replace('_', ' ')
#                                         logger.info(f"Processing hospital: {hospital_name} in {zipcode}, {city_name}")
#                                         hospital, hospital_created = get_or_create(Hospital, name=hospital_name, zipcode_id=zipcode_obj.id)
#                                         if hospital_created:
#                                             logger.info(f"Created new hospital: {hospital_name}")
#                                         else:
#                                             logger.info(f"Retrieved existing hospital: {hospital_name}")
#
#                 logger.info("Location data loaded successfully")
#
#         except Exception as e:
#             logger.error(f"An error occurred: {str(e)}")
#             logger.error(f"Current working directory: {os.getcwd()}")
#             logger.error(f"Python path: {sys.path}")
#             raise
#
#     # Define the task
#     load_location_data_task = load_location_data()
#
# # Instantiate the DAG
# dag = load_location_data_dag()