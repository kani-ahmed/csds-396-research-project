from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


@dag(
    'hospital_pricing_data_load_test',
    default_args=default_args,
    description='Test loading a single hospital pricing data CSV file into staging table',
    schedule_interval=None,
    catchup=False,
)
def hospital_pricing_data_load_test_dag():
    @task.external_python(task_id="load_hospital_pricing_data", python='/home/airflow/dbvenv/bin/python')
    def load_single_csv_file():
        import pandas as pd
        import os
        from sqlalchemy import create_engine
        from database import create_app
        from models.staging import StagingTable  # Updated import
        import logging

        # Set up logging
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        # Specify the exact file path
        file_path = "/opt/airflow/data/Hospital Price Transparency Data/OH/CLEVELAND/44106/UH_CLEVELAND_MEDICAL_CENTER/341567805_uh-cleveland-medical-center,-uh-seidman-cancer-center,-uh-rainbow-babies-&-children's-hospital_standardcharges.csv"

        def process_csv_file(file_path: str, db_engine):
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

            # Extract city, zipcode, and hospital from the file path
            path_parts = file_path.split(os.sep)
            state, city, zipcode, hospital = path_parts[-5:-1]
            hospital = hospital.replace('_', ' ')

            logger.info(f"File read successfully. Shape: {df.shape}")
            logger.info(f"Columns: {df.columns.tolist()}")

            # Map DataFrame columns to staging table columns
            df_mapped = pd.DataFrame({
                'cpt_code': df['Associated_Codes'],
                'cash_discount': df['Cash_Discount'],
                'deidentified_max_allowed': df['DeIdentified_Max_Allowed'],
                'deidentified_min_allowed': df['Deidentified_Min_Allowed'],
                'payer_id': df['payer'],
                'city_id': city,
                'zipcode_id': zipcode,
                'hospital_id': hospital,
                'description': df['description'],
                'gross_charge': df['Gross_Charge'],
                'payer_allowed_amount': df['Payer_Allowed_Amount']
            })

            logger.info(f"Mapped DataFrame. Shape: {df_mapped.shape}")

            # Insert data into staging table
            df_mapped.to_sql('staging_table', db_engine, if_exists='append', index=False)

            logger.info("Data successfully inserted into the database")

        # Create Flask app and application context
        app = create_app()
        with app.app_context():
            engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])

            # Create only the staging table
            StagingTable.__table__.create(engine, checkfirst=True)
            logger.info("Staging table created successfully")

            try:
                process_csv_file(file_path, engine)
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
            finally:
                engine.dispose()

    load_single_csv_file()


hospital_pricing_data_load_test_dag()