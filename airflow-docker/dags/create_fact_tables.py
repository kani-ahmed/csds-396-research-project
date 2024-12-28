# dags/create_warehouse_tables.py
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
    dag_id='create_warehouse_tables',
    default_args=default_args,
    description='A DAG to create the warehouse tables for Hospital Price Transparency data',
    schedule_interval=timedelta(days=1),
    catchup=False
)
def create_warehouse_tables_dag():
    @task.external_python(task_id="create_warehouse_tables", python='/home/airflow/dbvenv/bin/python')
    def create_warehouse_tables():
        import logging
        import os
        import sys
        from sqlalchemy import create_engine, inspect
        from extensions import create_app
        from models import FactCPTCode, FactLocation, FactHospital, FactPayer, FactPayerLocation, FactHospitalCharge, FactICDCPTMapping, User, FacilityDetails

        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        try:
            app = create_app()
            engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])

            # Create all tables
            FactCPTCode.__table__.create(engine, checkfirst=True)
            FactLocation.__table__.create(engine, checkfirst=True)
            FactHospital.__table__.create(engine, checkfirst=True)
            FactPayer.__table__.create(engine, checkfirst=True)
            FactPayerLocation.__table__.create(engine, checkfirst=True)
            FactHospitalCharge.__table__.create(engine, checkfirst=True)
            FactICDCPTMapping.__table__.create(engine, checkfirst=True)
            User.__table__.create(engine, checkfirst=True)
            FacilityDetails.__table__.create(engine, checkfirst=True)

            logger.info("All warehouse tables created successfully")

            # Log the list of tables
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            logger.info(f"Tables in database: {tables}")

            # Log the schema of each table
            for table_name in tables:
                columns = inspector.get_columns(table_name)
                logger.info(f"Schema for table {table_name}:")
                for column in columns:
                    logger.info(f"  - {column['name']}: {column['type']}")

        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            logger.error(f"Current working directory: {os.getcwd()}")
            logger.error(f"Python path: {sys.path}")
            raise

    create_warehouse_tables_task = create_warehouse_tables()


dag = create_warehouse_tables_dag()