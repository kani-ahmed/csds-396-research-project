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
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
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
#
# @dag(
#     dag_id='create_all_tables_into_db',
#     default_args=default_args,
#     description='A DAG to create tables for Hospital Price Transparency data',
#     schedule_interval=timedelta(days=1),
#     catchup=False
# )
# def hospital_price_transparency_dag():
#     @task.external_python(task_id="create_tables", python='/home/airflow/dbvenv/bin/python')
#     def create_tables():
#         import logging
#         from datetime import datetime, timedelta
#         import sys
#         import os
#
#         logging.basicConfig(level=logging.INFO)
#         logger = logging.getLogger(__name__)
#
#         try:
#             from extensions import db, create_app
#             from models import StagingTable
#
#             """
#             City, ZipCode, Hospital, Payer, CptTranslation, HospitalPricingData, IcdCptMapping, \
#                 ZipcodePayer
#             """
#
#             app = create_app()
#             with app.app_context():
#                 db.create_all()
#                 logger.info("All tables created successfully")
#
#                 # Log the list of tables
#                 inspector = db.inspect(db.engine)
#                 tables = inspector.get_table_names()
#                 logger.info(f"Created tables: {tables}")
#
#         except Exception as e:
#             logger.error(f"An error occurred: {str(e)}")
#             logger.error(f"Current working directory: {os.getcwd()}")
#             logger.error(f"Python path: {sys.path}")
#             raise
#
#     create_tables_task = create_tables()
#
#
# dag = hospital_price_transparency_dag()