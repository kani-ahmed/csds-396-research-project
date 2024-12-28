from airflow import DAG
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging
import sys
import os

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
    dag_id='create_staging_table',
    default_args=default_args,
    description='A DAG to create the staging table for Hospital Price Transparency data',
    schedule_interval=timedelta(days=1),
    catchup=False
)
def create_staging_table_dag():
    @task.external_python(task_id="create_staging_table", python='/home/airflow/dbvenv/bin/python')
    def create_staging_table():
        import logging
        from sqlalchemy import create_engine
        from extensions import create_app
        from models.staging import StagingTable
        import os
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        try:
            app = create_app()
            engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])

            # Create only the staging table
            StagingTable.__table__.create(engine, checkfirst=True)
            logger.info("Staging table created successfully")

            # Log the list of tables
            from sqlalchemy import inspect
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            logger.info(f"Tables in database: {tables}")

        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            logger.error(f"Current working directory: {os.getcwd()}")
            logger.error(f"Python path: {sys.path}")
            raise

    create_staging_table_task = create_staging_table()

dag = create_staging_table_dag()