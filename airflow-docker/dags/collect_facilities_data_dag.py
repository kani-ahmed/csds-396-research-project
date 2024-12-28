from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from typing import List, Dict
import logging
import requests
import csv
import json
import os
from tenacity import retry, stop_after_attempt, wait_exponential
from datetime import timedelta

# Suppress warnings from unverified HTTPS requests
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Define states
STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

# Base directory for output CSV file
BASE_DIR = '/opt/airflow/data/'

# Session configuration
SESSION_HEADERS = {
    'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
    'sec-ch-ua-mobile': '?0',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'sec-ch-ua-platform': '"macOS"',
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
    tags=['hospital_facility_data']
)
def hospital_facility_data_collection_dag():

    @task
    def collect_and_save_facilities():
        session = requests.Session()
        session.headers.update(SESSION_HEADERS)
        session.verify = False

        all_facilities = []
        for state in STATES:
            search_url = f"https://pts.patientrightsadvocatefiles.org/facility/search?searchstate={state}"
            try:
                response = make_request(session, search_url)
                facilities = response.json()
                all_facilities.extend(facilities)
                logging.info(f"Successfully retrieved {len(facilities)} facilities for state {state}")
            except Exception as e:
                logging.error(f"Failed to get facilities for state {state}: {str(e)}")

        # Save to CSV
        os.makedirs(BASE_DIR, exist_ok=True)
        csv_file = os.path.join(BASE_DIR, 'hospital_facilities.csv')

        fieldnames = ['id', 'name', 'address', 'city', 'state', 'zip', 'phone', 'beds', 'lat', 'long', 'ccn', 'url', 'files']

        with open(csv_file, 'w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for facility in all_facilities:
                if facility:
                    row = {field: facility.get(field, '') for field in fieldnames if field != 'files'}
                    row['files'] = json.dumps(facility.get('files', []))
                    writer.writerow(row)

        logging.info(f"Saved {len(all_facilities)} facilities to {csv_file}")

    collect_and_save_facilities()

hospital_facility_data_collection_dag()