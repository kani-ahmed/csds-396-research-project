from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'master_hospital_data_orchestrator',
    default_args=default_args,
    description='Orchestrator DAG for hospital data processing pipeline',
    schedule_interval=None,
    catchup=False,
    tags=['hospital_data', 'orchestrator'],
) as dag:

    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    with TaskGroup('initialize_tables') as initialize_tables:
        create_staging_table = TriggerDagRunOperator(
            task_id='create_staging_table',
            trigger_dag_id='create_staging_table_dag',
        )
        create_fact_tables = TriggerDagRunOperator(
            task_id='create_fact_tables',
            trigger_dag_id='create_fact_tables_dag',
        )
        [create_staging_table, create_fact_tables]

    with TaskGroup('process_hospital_data') as process_hospital_data:
        download_and_load_hospital_data = TriggerDagRunOperator(
            task_id='download_and_load_hospital_data',
            trigger_dag_id='hospital_data_download_dag',
        )
        load_hospital_pricing_data = TriggerDagRunOperator(
            task_id='load_hospital_pricing_data',
            trigger_dag_id='hospital_pricing_data_loader_dag',
        )
        with TaskGroup('transform_hospital_data') as transform_hospital_data:
            dbt_staging = TriggerDagRunOperator(
                task_id='dbt_staging',
                trigger_dag_id='dbt_staging_transformation_dag',
            )
            dbt_intermediate = TriggerDagRunOperator(
                task_id='dbt_intermediate',
                trigger_dag_id='dbt_intermediate_transformation_dag',
            )
            dbt_warehouse = TriggerDagRunOperator(
                task_id='dbt_warehouse',
                trigger_dag_id='dbt_warehouse_transformation_dag',
            )
            dbt_staging >> dbt_intermediate >> dbt_warehouse

        download_and_load_hospital_data >> load_hospital_pricing_data >> transform_hospital_data

    with TaskGroup('process_reference_data') as process_reference_data:
        with TaskGroup('download_reference_data') as download_reference_data:
            download_cpt = TriggerDagRunOperator(
                task_id='download_cpt_translations',
                trigger_dag_id='download_cpt_translations_dag',
            )
            download_icd = TriggerDagRunOperator(
                task_id='download_icd10cm_mappings',
                trigger_dag_id='download_icd-10-cm_mappings_dag',
            )
            [download_cpt, download_icd]

        with TaskGroup('transform_reference_data') as transform_reference_data:
            dbt_ref_staging = TriggerDagRunOperator(
                task_id='dbt_ref_staging',
                trigger_dag_id='dbt_ref_staging_transformation_dag',
            )
            dbt_ref_intermediate = TriggerDagRunOperator(
                task_id='dbt_ref_intermediate',
                trigger_dag_id='dbt_ref_intermediate_transformation_dag',
            )
            dbt_ref_warehouse = TriggerDagRunOperator(
                task_id='dbt_ref_warehouse',
                trigger_dag_id='dbt_ref_warehouse_transformation_dag',
            )
            dbt_ref_staging >> dbt_ref_intermediate >> dbt_ref_warehouse

        with TaskGroup('load_reference_data') as load_reference_data:
            load_cpt_data = TriggerDagRunOperator(
                task_id='load_cpt_data',
                trigger_dag_id='load_cpt_data_dag',
            )
            load_icd_data = TriggerDagRunOperator(
                task_id='load_icd_data',
                trigger_dag_id='load_icd_data_dag',
            )
            [load_cpt_data, load_icd_data]

        download_reference_data >> transform_reference_data >> load_reference_data

    dbt_production = TriggerDagRunOperator(
        task_id='dbt_production',
        trigger_dag_id='dbt_production_transformation_dag',
    )

    # Define the main task dependencies
    start >> initialize_tables >> process_hospital_data >> process_reference_data >> dbt_production >> end