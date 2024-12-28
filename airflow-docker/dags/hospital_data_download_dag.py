from airflow.decorators import dag, task, task_group
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from typing import List, Dict
import logging
import requests
import json
import re
from urllib.parse import unquote, urlparse
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Define states
STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

# S3 configuration
S3_BUCKET = 'health-data-v1'

# Session configuration
SESSION_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Sec-Fetch-Site': 'cross-site',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'sessionid': '5087494111016062868872034',
}

DOWNLOAD_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Sec-Fetch-Site': 'cross-site',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Dest': 'document',
    'host': 'storage.patientrightsadvocatefiles.org'
}

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def make_request(session, url, stream=False):
    response = session.get(url, stream=stream)
    response.raise_for_status()
    return response

@dag(
    default_args={'owner': 'airflow', 'retries': 3, 'retry_delay': timedelta(minutes=5)},
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['hospital_data']
)
def hospital_data_download_dag():
    def process_state(state: str):
        @task_group(group_id=f'process_state_{state}')
        def state_tasks():
            @task(task_id=f'get_facilities_{state}')
            def get_facilities() -> List[dict]:
                with requests.Session() as session:
                    session.headers.update(SESSION_HEADERS)
                    session.verify = False
                    search_url = f"https://pts.patientrightsadvocatefiles.org/facility/search?searchstate={state}"
                    try:
                        response = make_request(session, search_url)
                        return response.json()
                    except requests.RequestException as e:
                        logging.error(f"Failed to get facilities for state {state}: {str(e)}")
                        raise AirflowException(f"Failed to get facilities for state {state}")

            @task(task_id=f'download_facility_files_{state}')
            def download_facility_files(facility: dict) -> dict:
                s3_hook = S3Hook(aws_conn_id='aws_default')
                s3_client = s3_hook.get_client_type('s3')

                uploaded = []
                failed = []

                with requests.Session() as session:
                    session.headers.update(DOWNLOAD_HEADERS)
                    session.verify = False

                    def process_file(file):
                        download_url = f"https://storage.patientrightsadvocatefiles.org/{file['project']}/{file['storage']}"
                        safe_filename = re.sub(r'[\\/*?:"<>|]', "",
                                               unquote(urlparse(download_url).path.split('/')[-1])).replace(' ', '_')
                        s3_key = f"{state}_{facility['name']}_{safe_filename}"

                        try:
                            response = make_request(session, download_url, stream=True)
                            s3_client.upload_fileobj(response.raw, S3_BUCKET, s3_key)
                            return {"status": "success", "key": s3_key}
                        except Exception as e:
                            return {"status": "failed", "key": s3_key, "url": download_url, "error": str(e)}

                    with ThreadPoolExecutor(max_workers=10) as executor:
                        futures = [executor.submit(process_file, file) for file in facility["files"]]
                        for future in as_completed(futures):
                            result = future.result()
                            if result["status"] == "success":
                                uploaded.append(result["key"])
                                logging.info(f"Uploaded to S3: {result['key']}")
                            else:
                                failed.append(result)
                                logging.error(f"Failed to upload to S3: {result['key']}: {result['error']}")

                return {"state": state, "facility": facility["name"], "uploaded": len(uploaded), "failed": len(failed)}

            @task(task_id=f'aggregate_state_results_{state}')
            def aggregate_state_results(state_results: List[Dict]) -> Dict:
                total_uploaded = sum(result["uploaded"] for result in state_results)
                total_failed = sum(result["failed"] for result in state_results)
                return {
                    "state": state,
                    "uploaded": total_uploaded,
                    "failed": total_failed
                }

            facilities = get_facilities()
            facility_results = download_facility_files.expand(facility=facilities)
            return aggregate_state_results(facility_results)

        return state_tasks()

    @task
    def aggregate_final_results(results: List[Dict]) -> None:
        total_uploaded = sum(result["uploaded"] for result in results)
        total_failed = sum(result["failed"] for result in results)
        logging.info(f"Total files uploaded: {total_uploaded}")
        logging.info(f"Total files failed: {total_failed}")

        summary_data = json.dumps({
            "total_uploaded": total_uploaded,
            "total_failed": total_failed,
            "state_results": results
        }).encode('utf-8')

        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_client = s3_hook.get_client_type('s3')
        s3_key = "download_summary.json"
        s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=summary_data)
        logging.info(f"Saved summary to S3: {s3_key}")

    state_results = []
    for state in STATES:
        state_result = process_state(state)
        state_results.append(state_result)

    aggregate_final_results(state_results)

hospital_data_download_dag()

# from airflow.decorators import dag, task, task_group
# from airflow.utils.dates import days_ago
# from airflow.exceptions import AirflowException
# from typing import List, Dict
# import logging
# import requests
# import json
# import os
# import re
# from urllib.parse import unquote, urlparse
# from tenacity import retry, stop_after_attempt, wait_exponential
# from datetime import timedelta
#
# # Suppress warnings from unverified HTTPS requests
# requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
#
# # Define states
# STATES = [
#     "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
#     "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
#     "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
#     "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
#     "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
# ]
#
# # Base directory for downloaded files
# BASE_DIR = '/opt/airflow/data/Hospital Price Transparency Data'
#
# # Session configuration
# SESSION_HEADERS = {
#     'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
#     'sec-ch-ua-mobile': '?0',
#     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
#     'sec-ch-ua-platform': '"macOS"',
#     'Accept': '*/*',
#     'Sec-Fetch-Site': 'cross-site',
#     'Sec-Fetch-Mode': 'cors',
#     'Sec-Fetch-Dest': 'empty',
#     'sessionid': '5087494111016062868872034',
# }
#
# DOWNLOAD_HEADERS = {
#     'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
#     'sec-ch-ua-mobile': '?0',
#     'sec-ch-ua-platform': '"macOS"',
#     'Upgrade-Insecure-Requests': '1',
#     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
#     'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
#     'Sec-Fetch-Site': 'cross-site',
#     'Sec-Fetch-Mode': 'navigate',
#     'Sec-Fetch-User': '?1',
#     'Sec-Fetch-Dest': 'document',
#     'host': 'storage.patientrightsadvocatefiles.org'
# }
#
# @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
# def make_request(session, url, stream=False):
#     response = session.get(url, stream=stream)
#     response.raise_for_status()
#     return response
#
# @dag(
#     default_args={'owner': 'airflow', 'retries': 3, 'retry_delay': timedelta(minutes=5)},
#     schedule_interval='@daily',
#     start_date=days_ago(1),
#     catchup=False,
#     tags=['hospital_data']
# )
# def hospital_data_download_dag():
#
#     def process_state(state: str):
#         @task_group(group_id=f'process_state_{state}')
#         def state_tasks():
#             @task(task_id=f'get_facilities_{state}')
#             def get_facilities() -> List[dict]:
#                 session = requests.Session()
#                 session.headers.update(SESSION_HEADERS)
#                 session.verify = False
#
#                 search_url = f"https://pts.patientrightsadvocatefiles.org/facility/search?searchstate={state}"
#                 try:
#                     response = make_request(session, search_url)
#                     return response.json()
#                 except requests.RequestException as e:
#                     logging.error(f"Failed to get facilities for state {state}: {str(e)}")
#                     raise AirflowException(f"Failed to get facilities for state {state}")
#
#             @task(task_id=f'download_facility_files_{state}')
#             def download_facility_files(facility: dict) -> dict:
#                 session = requests.Session()
#                 session.headers.update(DOWNLOAD_HEADERS)
#                 session.verify = False
#
#                 city = facility["city"]
#                 zip_code = facility["zip"]
#                 facility_name = re.sub(r'[\\/*?:"<>|]', "", facility["name"]).replace(' ', '_')
#                 directory_path = os.path.join(BASE_DIR, state, city, zip_code, facility_name)
#                 os.makedirs(directory_path, exist_ok=True)
#
#                 downloaded = []
#                 failed = []
#
#                 for file in facility["files"]:
#                     download_url = f"https://storage.patientrightsadvocatefiles.org/{file['project']}/{file['storage']}"
#                     safe_filename = re.sub(r'[\\/*?:"<>|]', "",
#                                            unquote(urlparse(download_url).path.split('/')[-1])).replace(' ', '_')
#                     save_path = os.path.join(directory_path, safe_filename)
#
#                     try:
#                         response = make_request(session, download_url, stream=True)
#                         with open(save_path, "wb") as f:
#                             for chunk in response.iter_content(chunk_size=8192):
#                                 f.write(chunk)
#                         downloaded.append(save_path)
#                         logging.info(f"Downloaded {save_path}")
#                     except Exception as e:
#                         failed.append({"path": save_path, "url": download_url, "error": str(e)})
#                         logging.error(f"Failed to download {save_path}: {e}")
#
#                 return {"state": state, "facility": facility["name"], "downloaded": len(downloaded), "failed": len(failed)}
#
#             @task(task_id=f'aggregate_state_results_{state}')
#             def aggregate_state_results(state_results: List[Dict]) -> Dict:
#                 total_downloaded = sum(result["downloaded"] for result in state_results)
#                 total_failed = sum(result["failed"] for result in state_results)
#                 return {
#                     "state": state,
#                     "downloaded": total_downloaded,
#                     "failed": total_failed
#                 }
#
#             facilities = get_facilities()
#             facility_results = download_facility_files.expand(facility=facilities)
#             return aggregate_state_results(facility_results)
#
#         return state_tasks()
#
#     @task
#     def aggregate_final_results(results: List[Dict]) -> None:
#         total_downloaded = sum(result["downloaded"] for result in results)
#         total_failed = sum(result["failed"] for result in results)
#         logging.info(f"Total files downloaded: {total_downloaded}")
#         logging.info(f"Total files failed: {total_failed}")
#
#         # Save results to a file
#         with open(os.path.join(BASE_DIR, 'download_summary.json'), 'w') as f:
#             json.dump({
#                 "total_downloaded": total_downloaded,
#                 "total_failed": total_failed,
#                 "state_results": results
#             }, f, indent=2)
#
#     state_results = []
#     for state in STATES:
#         state_result = process_state(state)
#         state_results.append(state_result)
#
#     aggregate_final_results(state_results)
#
# hospital_data_download_dag()