# dags/load_icd_cpt_mappings_dag.py
from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import sys

# Add the project root to the Python path
sys.path.append('/opt/airflow')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='load_icd_cpt_mappings',
    default_args=default_args,
    description='A DAG to load ICD-CPT mappings data',
    schedule_interval=timedelta(days=1),
    catchup=False
)
def load_icd_cpt_mappings_dag():
    @task.external_python(task_id="load_icd_cpt_mappings", python='/home/airflow/dbvenv/bin/python')
    def load_icd_cpt_mappings():
        import logging
        import os
        import sys
        import csv
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from extensions import create_app
        from models import FactICDCPTMapping, FactCPTCode

        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        try:
            app = create_app()
            engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
            Session = sessionmaker(bind=engine)

            with Session() as session:
                code_mappings_directory = '/opt/airflow/data/COLLECTED-CPT-ICD10-CM-CODE_MAPPINGS'

                # Get all CPT codes from the FactCPTCode table and store in a set for quick lookup
                existing_cpt_codes = {str(cpt.cpt_code) for cpt in session.query(FactCPTCode).all()}

                files = os.listdir(code_mappings_directory)
                for file in files:
                    if file.endswith('_code_mappings.csv'):
                        cpt_code = file.split('_')[0]
                        if cpt_code not in existing_cpt_codes:
                            logger.warning(f"CPT code {cpt_code} from file {file} does not exist in FactCPTCode table. Skipping file.")
                            continue

                        csv_file_path = os.path.join(code_mappings_directory, file)
                        with open(csv_file_path, newline='') as csvfile:
                            reader = csv.DictReader(csvfile)
                            for row in reader:
                                icd_code = row['Diagnosis Code']
                                cpt_id = session.query(FactCPTCode).filter_by(cpt_code=int(cpt_code)).first().cpt_id
                                icd_cpt_mapping = FactICDCPTMapping(icd_code=icd_code, cpt_id=cpt_id)
                                session.add(icd_cpt_mapping)

                        try:
                            session.commit()
                            logger.info(f"Successfully committed ICD-CPT mappings from {file}")
                        except Exception as e:
                            session.rollback()
                            logger.error(f"An error occurred while inserting data from {file}: {e}")

                logger.info("ICD-CPT mappings data loaded successfully")

        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            logger.error(f"Current working directory: {os.getcwd()}")
            logger.error(f"Python path: {sys.path}")
            raise

    load_icd_cpt_mappings_task = load_icd_cpt_mappings()

dag = load_icd_cpt_mappings_dag()