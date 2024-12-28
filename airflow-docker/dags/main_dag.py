# from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.dummy_operator import DummyOperator
# from datetime import datetime
#
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 7, 10),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
# }
#
# with DAG(
#     'main_dag',
#     default_args=default_args,
#     description='A DAG to run all pre-cleaning, cleaning and grouping scripts',
#     schedule_interval=None,
#     catchup=False,
# ) as dag:
#     fetch_hospital_files = BashOperator(
#         task_id='download_hospital_data',
#         bash_command='python3 /opt/airflow/dags/Fetch-Hospital-Files.py',
#     )
#
#     count_number_of_occurrences_header_in_csv_and_json = BashOperator(
#         task_id='count_number_of_occurrences_header_in_csv_and_json',
#         bash_command='python3 /opt/airflow/data-cleaning-pipeline/pre-cleaning-analysis-scripts/count-num-occurrences-header-in-csv-and-json.py',
#     )
#
#     # find_any = BashOperator(
#     #     task_id='find_any',
#     #     bash_command='python3 /opt/airflow/data-cleaning-pipeline/pre-cleaning-analysis-scripts/find-any.py',
#     # )
#
#     find_cpt_codes = BashOperator(
#         task_id='find_cpt_codes',
#         bash_command='python3 /opt/airflow/data-cleaning-pipeline/pre-cleaning-analysis-scripts/find-cpt.py',
#     )
#
#     generate_collected_data_statistics = BashOperator(
#         task_id='generate_collected_data_statistics',
#         bash_command='python3 /opt/airflow/data-cleaning-pipeline/pre-cleaning-analysis-scripts/generate-collected-data-statistics.py',
#     )
#
#     most_common_headers = BashOperator(
#         task_id='most_common_headers',
#         bash_command='python3 /opt/airflow/data-cleaning-pipeline/pre-cleaning-analysis-scripts/most-common-headers.py',
#     )
#
#     move_relevant_files = BashOperator(
#         task_id='move_relevant_files',
#         bash_command='python3 /opt/airflow/data-cleaning-pipeline/pre-cleaning-analysis-scripts/move-relevant-files.py',
#     )
#
#     # Cleaning and grouping tasks
#     consolidate_same_header_csv_files = BashOperator(
#         task_id='consolidate_same_header_csv_files',
#         bash_command='python3 /opt/airflow/data-cleaning-pipeline/cleaning-grouping-scripts/consolidate-same-header-csv-files-into-one-file.py',
#     )
#
#     group_codes_from_all_files = BashOperator(
#         task_id='group_codes_from_all_files',
#         bash_command='python3 /opt/airflow/data-cleaning-pipeline/cleaning-grouping-scripts/group-codes-from-all-files.py',
#     )
#
#     extract_insurance_companies = BashOperator(
#         task_id='extract_insurance_companies',
#         bash_command='python3 /opt/airflow/data-cleaning-pipeline/cleaning-grouping-scripts/extract_insurance-companies.py',
#     )
#
#     extract_searchable_fields = BashOperator(
#         task_id='extract_searchable_fields',
#         bash_command='python3 /opt/airflow/data-cleaning-pipeline/cleaning-grouping-scripts/extract-searchable-fields.py',
#     )
#
#     dummy_operator = DummyOperator(task_id='end_dummy_task')
#
#     fetch_hospital_files >> [count_number_of_occurrences_header_in_csv_and_json, find_cpt_codes, generate_collected_data_statistics, most_common_headers] >> move_relevant_files
#     move_relevant_files >> [group_codes_from_all_files, extract_insurance_companies, consolidate_same_header_csv_files] >> extract_searchable_fields