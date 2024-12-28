from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import os
from typing import List

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

@dag(
    'hospital_pricing_data_load_all',
    default_args=default_args,
    description='Load all matching hospital pricing data CSV files into staging table',
    schedule_interval=None,
    catchup=False,
)
def hospital_pricing_data_load_all_dag():

    @task
    def get_states() -> List[str]:
        BASE_DIR = "/opt/airflow/data/Hospital Price Transparency Data"
        return [d for d in os.listdir(BASE_DIR) if os.path.isdir(os.path.join(BASE_DIR, d))]

    @task
    def get_state_files(state: str) -> List[str]:
        BASE_DIR = "/opt/airflow/data/Hospital Price Transparency Data"
        state_dir = os.path.join(BASE_DIR, state)
        csv_files = []
        for root, dirs, files in os.walk(state_dir):
            for file in files:
                if file.endswith('.csv'):
                    csv_files.append(os.path.join(root, file))
        return csv_files

    @task
    def flatten_file_list(nested_list: List[List[str]]) -> List[str]:
        return [item for sublist in nested_list for item in sublist]

    @task.external_python(python='/home/airflow/dbvenv/bin/python')
    def process_file(file_path: str):
        import pandas as pd
        from sqlalchemy import create_engine
        from database import create_app
        from models.staging import StagingTable
        import logging
        import os

        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        REQUIRED_COLUMNS = {
            'Associated_Codes', 'Cash_Discount', 'DeIdentified_Max_Allowed',
            'Deidentified_Min_Allowed', 'description', 'Gross_Charge',
            'payer', 'Payer_Allowed_Amount'
        }

        def is_valid_csv(file_path):
            try:
                df = pd.read_csv(file_path, nrows=0)
                found_columns = set(df.columns)
                logger.info(f"Found columns in {file_path}: {found_columns}")
                return set(df.columns) >= REQUIRED_COLUMNS
            except Exception as e:
                logger.error(f"Error validating CSV {file_path}: {str(e)}")
                return False

        if not is_valid_csv(file_path):
            logger.warning(f"Skipping invalid file: {file_path}")
            return

        encodings = ['utf-8', 'iso-8859-1', 'cp1252']
        df = None

        for encoding in encodings:
            try:
                logger.info(f"Attempting to read file with encoding: {encoding}")
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                logger.info(f"Successfully read file with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                logger.warning(f"Failed to read file with encoding: {encoding}")
                continue

        if df is None:
            logger.error(f"Unable to read file {file_path} with any of the tried encodings.")
            return

        path_parts = file_path.split(os.sep)
        state, city, zipcode, hospital = path_parts[-5:-1]
        hospital = hospital.replace('_', ' ')

        logger.info(f"File read successfully. Shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}")

        df_mapped = pd.DataFrame({
            'cpt_code': df['Associated_Codes'],
            'cash_discount': df['Cash_Discount'],
            'deidentified_max_allowed': df['DeIdentified_Max_Allowed'],
            'deidentified_min_allowed': df['Deidentified_Min_Allowed'],
            'payer_id': df['payer'],
            'state_id': state,
            'city_id': city,
            'zipcode_id': zipcode,
            'hospital_id': hospital,
            'description': df['description'],
            'gross_charge': df['Gross_Charge'],
            'payer_allowed_amount': df['Payer_Allowed_Amount']
        })

        logger.info(f"Mapped DataFrame. Shape: {df_mapped.shape}")

        app = create_app()
        with app.app_context():
            engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
            StagingTable.__table__.create(engine, checkfirst=True)
            df_mapped.to_sql('staging_table', engine, if_exists='append', index=False)
            logger.info(f"Data successfully inserted into the database for file: {file_path}")

    states = get_states()
    state_files = get_state_files.expand(state=states)
    flattened_files = flatten_file_list(state_files)
    process_file.expand(file_path=flattened_files)

hospital_pricing_data_load_all_dag()