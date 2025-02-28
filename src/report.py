from dotenv import load_dotenv
import requests
import base64
import gzip
import zipfile
import io
import os
import json
from datetime import datetime, timedelta

from models.db import check_and_write_event_to_db, connect_to_db

load_dotenv(override=True)

PROJECT_ID = os.getenv('PROJECT_ID')
API_KEY = os.getenv('API_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
START_DATE = os.getenv('START_DATE')
END_DATE = os.getenv('END_DATE')


def fetch_amplitude_data(start_time, end_time, project_id, api_key, secret_key):
    """
    Fetches event data from Amplitude Export API for a given time range.
    """
    url = f"https://amplitude.com/api/2/export?start={start_time}&end={end_time}"
    auth_string = f"{api_key}:{secret_key}"
    auth_bytes = auth_string.encode('utf-8')
    auth_base64 = base64.b64encode(auth_bytes).decode('utf-8')
    headers = {"Authorization": f"Basic {auth_base64}"}

    try:
        response = requests.get(url, headers=headers, stream=True)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)

        # Local directory to write temp gzip and JSON files
        directory_path = './output/' + start_time

        # Decompress the ZIP folder, then each individual gzipped file
        with zipfile.ZipFile(io.BytesIO(response.content), 'r') as zip_ref:
            zip_ref.extractall(directory_path)
            subfolder_path = directory_path + '/' + '/' + project_id
            print('Unzipped to ' + subfolder_path)
            for filename in os.listdir(subfolder_path):
                file_path = os.path.join(subfolder_path, filename)
                if os.path.isfile(file_path):  # Ensure it's a file, not a subdirectory
                    print(f"Found file: {filename}")
                    # Read the file content
                    try:
                        decompressed_data = gzip.GzipFile(file_path, mode='rb')
                        # Read and process each line (JSON object)
                        for line in decompressed_data:
                            try:
                                event = json.loads(line)
                                yield event
                            except json.JSONDecodeError as e:
                                print(f"Error decoding JSON: {e}")
                                continue
                    except Exception as e:
                        print(f"Error reading {filename}: {e}")

    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return
    except gzip.BadGzipFile as e:
        print(f"Gzip decompression failed: {e}")
        return


def generate_intervals(start_date, end_date):
    """
    Generates a list of daily time intervals between start_date and end_date.
    """
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    intervals = []
    current = start
    while current < end:
        next_ = current  # + timedelta(days=1) # valid increments - days, hours
        intervals.append((current.strftime("%Y%m%d") + 'T00',
                         (next_.strftime("%Y%m%d")) + 'T23'))
        current = next_ + timedelta(days=1)
    return intervals


if __name__ == '__main__':
    daily_intervals = generate_intervals(START_DATE, END_DATE)
    connect_to_db()
    for start, end in daily_intervals:
        for event in fetch_amplitude_data(start, end, PROJECT_ID, API_KEY, SECRET_KEY):
            check_and_write_event_to_db(event)
