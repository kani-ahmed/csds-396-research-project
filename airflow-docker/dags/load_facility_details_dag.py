from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import sys
import csv
import os

# Add the project root to the Python path
sys.path.append('/opt/airflow')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 7, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
}

# Base directory for input CSV file
BASE_DIR = '/opt/airflow/data/'
CSV_FILE = os.path.join(BASE_DIR, 'hospital_facilities.csv')

@dag(
    dag_id='load_hospital_facility_data',
    default_args=default_args,
    description='A DAG to load hospital facility data from CSV to database',
    schedule_interval='@daily',
    catchup=False
)
def load_hospital_facility_data_dag():
    @task
    def load_csv_to_database():
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from extensions import create_app
        from models import FacilityDetails

        logger.info("Starting load_csv_to_database task")

        try:
            app = create_app()
            engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
            Session = sessionmaker(bind=engine)

            with Session() as db_session:
                with open(CSV_FILE, 'r') as file:
                    csv_reader = csv.DictReader(file)
                    for row in csv_reader:
                        facility_details = FacilityDetails(
                            facility_id=row['id'],
                            facility_name=row['name'],
                            address=row['address'],
                            city=row['city'],
                            state=row['state'],
                            zip_code=row['zip'],
                            phone=row['phone'],
                            beds=row['beds'] if row['beds'] else None,
                            latitude=row['lat'] if row['lat'] else None,
                            longitude=row['long'] if row['long'] else None,
                            ccn=row['ccn']
                        )
                        db_session.merge(facility_details)

                    db_session.commit()
                    logger.info("Successfully loaded data from CSV to database")

        except Exception as e:
            logger.error(f"An error occurred while loading CSV to database: {str(e)}")
            raise

    load_csv_to_database_task = load_csv_to_database()

dag = load_hospital_facility_data_dag()