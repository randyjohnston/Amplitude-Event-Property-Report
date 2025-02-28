# Amplitude Event Property Report

## Purpose

Generates a local SQLite database `properties.db` of Amplitude taxonomy (events, event properties, event property values, user properties, user property values) along with volumes, first seen date, and last seen date. Assists in identifying high cardinality or messy event and user property values.

## Setup

`.env` file setup:
  ```bash
    PROJECT_ID = "424706"
    API_KEY = "ae0525..."
    SECRET_KEY = "045a2..."
    START_DATE = "20230228"
    END_DATE = "20250228"
  ```

## Usage

Recommend using [venv](https://docs.python.org/3/library/venv.html) to create a virtual environment:
  ```bash
    python -m venv venv
    pip install -r requirements.txt
    source venv/bin/activate
  ```

To run the report:
  ```bash
    python src/report.py
  ```
  
Use [DB Browser for SQLite](https://sqlitebrowser.org/) to browse `properties.db` in the project directory, and query the following output tables:
- events
- event_properties
- event_properties_values
- user_properties
- user_properties_values

![event_properties table screenshot](/docs/event_properites_sample.png "event_properties table")


## Known Issues
- Temp output files are generated at `/output` of the project repository and are not automatically cleaned up; to avoid taking up too much drive storage space or persisting sensitive information these need to be manually removed
- Arrays and object arrays are assumed to be a string and may result in a lot of false positive unique property values
