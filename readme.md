# Airflow Podcast Downloader

This project uses Apache Airflow to automatically fetch, store episode details, and download audio files from a podcast RSS feed. It's currently set up for the "Marketplace" podcast from publicradio.org.

## Features

*   **Automated Fetching:** Regularly fetches the latest podcast episodes from a specified RSS feed.
*   **Metadata Storage:** Parses episode information (link, title, publication date, description) and stores it in an SQLite database.
*   **Duplicate Prevention:** Ensures that episodes are not processed or downloaded multiple times by checking against existing records in the database (using the episode link as a primary key).
*   **Audio Download:** Downloads the MP3 audio files for new episodes.
*   **Idempotent Downloads:** Checks if an audio file already exists before attempting to download it again.
*   **Scheduled Execution:** Configured to run daily using Airflow's scheduling.
*   **TaskFlow API & Traditional Operators:** Demonstrates both the modern TaskFlow API (`@task` decorator) and traditional Airflow operators (`SQLExecuteQueryOperator`).


## Prerequisites

*   **Apache Airflow:** A running Airflow environment (version 2.x recommended).
*   **Python 3.x**
*   **Required Python Packages:**
    *   `apache-airflow`
    *   `apache-airflow-providers-sqlite`
    *   `apache-airflow-providers-common-sql`
    *   `requests`
    *   `xmltodict`
    *   `pendulum`


## Usage

*   The DAG will run automatically based on its `@daily` schedule.
*   You can also trigger it manually through the Airflow UI.
*   Downloaded MP3 files will appear in the `episodes/` directory you created.
*   Episode metadata will be stored in the SQLite database file specified in your `podcasts` connection.

## Project Structure (Example)

```
your_airflow_project/
├── dags/
│ └── podcost_summary.py <-- Your DAG file
├── episodes/ <-- Directory for downloaded MP3s 
├── episodes.db <-- SQLite database file 
├── requirements.txt <-- List of Python dependencies
└── README.md
```
---

