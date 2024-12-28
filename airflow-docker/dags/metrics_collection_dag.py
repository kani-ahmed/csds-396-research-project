from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from typing import List, Dict, Optional
import logging
import requests
import json
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import timedelta

requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Define states
STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

# Session configuration
SESSION_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Sec-Fetch-Site': 'cross-site',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'sessionid': '5087494111016062868872034',
}


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def make_request(session, url):
    response = session.get(url)
    response.raise_for_status()
    return response


@dag(
    default_args={'owner': 'airflow', 'retries': 3, 'retry_delay': timedelta(minutes=5)},
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['hospital_data_metrics']
)
def hospital_data_metrics_dag():
    @task
    def get_state_metrics(state: str) -> Optional[Dict]:
        with requests.Session() as session:
            session.headers.update(SESSION_HEADERS)
            session.verify = False
            search_url = f"https://pts.patientrightsadvocatefiles.org/facility/search?searchstate={state}"
            try:
                response = make_request(session, search_url)
                facilities = response.json()

                total_facilities = len(facilities)
                total_files = 0
                total_beds = 0
                total_size = 0  # in bytes

                for facility in facilities:
                    total_files += len(facility['files'])
                    total_beds += int(facility['beds']) if facility['beds'].isdigit() else 0
                    for file in facility['files']:
                        total_size += int(file.get('size', 0))

                return {
                    "state": state,
                    "facilities": total_facilities,
                    "files": total_files,
                    "beds": total_beds,
                    "size": total_size
                }
            except Exception as e:
                logging.error(f"Failed to get facilities for state {state}: {str(e)}")
                return None

    @task
    def aggregate_metrics(state_metrics: List[Optional[Dict]]) -> Dict:
        valid_metrics = [metric for metric in state_metrics if metric is not None]

        total_facilities = sum(metric['facilities'] for metric in valid_metrics)
        total_files = sum(metric['files'] for metric in valid_metrics)
        total_beds = sum(metric['beds'] for metric in valid_metrics)
        total_size = sum(metric['size'] for metric in valid_metrics)
        total_tasks = len(STATES) + 1
        failed_states = len(STATES) - len(valid_metrics)

        metrics = {
            "total_facilities": total_facilities,
            "total_files": total_files,
            "total_beds": total_beds,
            "total_size_bytes": total_size,
            "total_size_gb": round(total_size / (1024 * 1024 * 1024), 2),
            "total_tasks": total_tasks,
            "states_processed": len(valid_metrics),
            "failed_states": failed_states,
            "state_breakdown": valid_metrics
        }

        logging.info(json.dumps(metrics, indent=2))
        return metrics

    state_metrics = get_state_metrics.expand(state=STATES)
    aggregate_metrics(state_metrics)


hospital_data_metrics_dag()