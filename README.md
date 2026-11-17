# Spotify AWS Data Engineering Pipeline

## 1. Project Overview

The **Spotify AWS Data Engineering Pipeline** is a cloud-based data engineering project that demonstrates how music streaming data can be stored, organized, queried, and analyzed using AWS services, PostgreSQL, Python, and Apache Airflow.

The project uses three datasets representing songs, users, and streaming activities. Amazon S3 provides cloud-based storage for the raw CSV datasets, while PostgreSQL provides a relational database for analytical queries.

Apache Airflow orchestrates scheduled analytics tasks, retrieves results from PostgreSQL, and generates a consolidated Spotify analytics report.

### Project Objectives

- Store raw Spotify datasets in Amazon S3.
- Organize data using structured S3 prefixes.
- Store and query relational Spotify data using PostgreSQL.
- Automate analytics workflows using Apache Airflow.
- Analyze listening activity, popular songs, and user behavior.
- Generate consolidated analytics reports.
- Practice AWS resource management, security, and cloud operations.

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Amazon EC2 | Cloud computing environment hosting the project |
| Amazon S3 | Storage for raw Spotify CSV datasets |
| Ubuntu Linux | Server operating system |
| Python | Data processing and automation |
| Apache Airflow 2.10.5 | Workflow orchestration and scheduling |
| PostgreSQL | Relational database and analytics queries |
| Psycopg2 | Python PostgreSQL database driver |
| Git and GitHub | Source control and project documentation |

## 3. Project Architecture

The project uses Amazon S3 for raw data storage, PostgreSQL for analytical queries, and Apache Airflow for workflow orchestration.

### Architecture Components

**Amazon EC2**
- Hosts the Ubuntu environment.
- Runs Apache Airflow.
- Hosts PostgreSQL.
- Executes Python scripts and analytics workflows.

**Amazon S3**
- Stores the original Spotify CSV datasets.
- Organizes files into raw, processed, and archived prefixes.

**PostgreSQL**
- Stores relational Spotify datasets.
- Supports SQL joins, aggregations, and analytical queries.

**Apache Airflow**
- Schedules analytics workflows.
- Executes Python tasks.
- Connects to PostgreSQL through an Airflow connection.
- Collects task results using XCom.
- Generates the final analytics report.

### Simplified Architecture

```text
                 AMAZON AWS CLOUD
                        |
                 Amazon EC2
                 Ubuntu Server
                        |
             +----------+----------+
             |                     |
         Amazon S3             PostgreSQL
             |                     |
       Raw CSV Data          Spotify Database
             |                     |
             |               SQL Analytics
             |                     |
             +----------+----------+
                        |
                  Apache Airflow
                        |
                Analytics Workflow
                        |
               Five Python Tasks
                        |
              XCom Result Collection
                        |
              Analytics Report
                        |
           spotify_analytics_report.txt
```

**Note:** The current Airflow DAG performs PostgreSQL analytics and generates a local report. S3 currently holds the raw datasets; automated S3-to-PostgreSQL ingestion and report uploads to S3 are not yet implemented in this DAG.

## 4. Amazon S3 Storage Structure

The project uses the following S3 bucket:

`spotify-data-pipeline-179512357941-20261116`

The bucket is organized into three main prefixes:

```text
spotify-data-pipeline/
│
├── raw/
│   ├── songs/
│   │   └── songs.csv
│   │
│   ├── streams/
│   │   └── streams.csv
│   │
│   └── users/
│       └── users.csv
│
├── processed/
│   ├── songs/
│   ├── streams/
│   └── users/
│
└── archived/
    └── streams/
```

### S3 Responsibilities

- `raw/`: Stores original CSV datasets.
- `processed/`: Reserved for transformed datasets.
- `archived/`: Reserved for historical datasets.

The raw datasets currently confirmed in S3 are:

- `raw/songs/songs.csv`
- `raw/streams/streams.csv`
- `raw/users/users.csv`

## 5. Dataset Description

The project uses three CSV datasets.

### Songs Dataset

File: `songs.csv`

Contains information about music tracks.

Important columns:

| Column | Description |
|---|---|
| track_id | Unique track identifier |
| artists | Artist name |
| album_name | Album title |
| track_name | Song title |
| popularity | Popularity score |
| duration_ms | Song duration |
| track_genre | Music genre |
| danceability | Danceability score |
| energy | Energy score |

### Streams Dataset

File: `streams.csv`

Contains listening activity.

| Column | Description |
|---|---|
| user_id | Listener identifier |
| track_id | Track identifier |
| listen_time | Date and time of listening activity |

### Users Dataset

File: `users.csv`

Contains listener information.

| Column | Description |
|---|---|
| user_id | Unique user identifier |
| user_name | Listener name |
| user_age | Listener age |
| user_country | Listener country |
| created_at | Account creation date |

## 6. PostgreSQL Database

Database name:

`spotify_analytics`

The database contains three main relational datasets:

- `songs`
- `streams`
- `users`

### Database Relationships

```text
songs
  |
  | track_id
  |
streams
  |
  | user_id
  |
users
```

The streams table connects listening activity to both songs and users.

These relationships allow analytical SQL queries across the datasets.

## 7. Apache Airflow Workflow

The project uses Apache Airflow 2.10.5 to orchestrate Spotify analytics.

DAG name:

`spotify_analytics_dag`

Schedule:

`@daily`

Catchup:

`False`

The workflow contains five PythonOperator tasks.

### Task 1: Most Streamed Songs

Function:

`get_most_streamed_songs()`

Uses SQL joins and aggregation to identify songs with the highest number of streams.

### Task 2: Streams by Country

Function:

`get_streams_by_country()`

Analyzes listening activity across different countries.

### Task 3: Streams by User

Function:

`get_streams_by_user()`

Identifies listening activity by individual users.

### Task 4: Song Popularity

Function:

`get_song_popularity()`

Calculates:

- Average song popularity.
- Highest popularity score.
- Lowest popularity score.
- Total number of songs.

### Task 5: Generate Analytics Report

Function:

`generate_analytics_report()`

Collects results from the previous four tasks using Airflow XCom and generates a consolidated text report.

Output:

`reports/spotify_analytics_report.txt`

### Workflow Dependencies

The four analytical tasks run independently before the final report task.

```text
get_most_streamed_songs ----+
                            |
get_streams_by_country -----+
                            |
get_streams_by_user --------+----> generate_analytics_report
                            |
get_song_popularity --------+
```

## 8. Airflow PostgreSQL Connection

The project uses the Airflow connection ID:

`spotify_postgres`

Connection configuration:

| Parameter | Value |
|---|---|
| Connection ID | spotify_postgres |
| Connection Type | PostgreSQL |
| Host | localhost |
| Database | spotify_analytics |
| Username | airflow_user |
| Port | 5432 |

The PostgreSQL password is entered interactively using Python's `getpass()` function.

The connection is configured through:

`scripts/create_airflow_connection.py`

The connection can be tested using:

`scripts/test_spotify_connection.py`

## 9. Project Directory Structure

```text
spotify-aws-data-pipeline/
│
├── dags/
│   └── spotify_analytics_dag.py
│
├── data/
│   ├── songs.csv
│   ├── streams.csv
│   └── users.csv
│
├── scripts/
│   ├── create_airflow_connection.py
│   └── test_spotify_connection.py
│
├── reports/
│   └── spotify_analytics_report.txt
│
├── requirements.txt
├── .gitignore
└── README.md
```

Virtual environments, generated reports, credentials, and sensitive configuration files are excluded from version control.

## 10. Installation and Configuration

### Prerequisites

- Ubuntu Linux
- Python 3
- PostgreSQL
- Apache Airflow 2.10.5
- Amazon AWS account
- AWS CLI
- Access to the project S3 bucket

### Step 1: Clone the Repository

```bash
git clone https://github.com/husmankol-cloud/spotify-aws-data-pipeline.git

cd spotify-aws-data-pipeline
```

### Step 2: Create a Python Virtual Environment

```bash
python3 -m venv airflow_venv

source airflow_venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Configure AWS Access

Configure AWS credentials using an appropriate IAM role or secure AWS CLI authentication method.

Verify S3 access:

```bash
aws s3 ls
```

### Step 5: Configure PostgreSQL

Create the project database and configure the PostgreSQL user with the required permissions.

Database:

`spotify_analytics`

### Step 6: Configure Airflow

Initialize and configure the Airflow metadata database and webserver/scheduler according to the Airflow installation environment.

Ensure the DAG directory is configured correctly.

### Step 7: Configure PostgreSQL Connection

Run:

```bash
python scripts/create_airflow_connection.py
```

Enter the PostgreSQL password when prompted.

### Step 8: Test PostgreSQL Connection

Run:

```bash
python scripts/test_spotify_connection.py
```

A successful connection displays the number of songs found in the database.

### Step 9: Run the Analytics DAG

Start the Airflow scheduler and webserver using your configured Airflow environment.

Enable `spotify_analytics_dag` in the Airflow interface and trigger a run or wait for its daily schedule.

## 11. Security and Operational Practices

Security practices used or recommended for this project include:

- Use IAM roles for EC2 access to AWS services where possible.
- Follow least-privilege IAM permissions.
- Avoid storing AWS access keys in source code.
- Keep database passwords out of Python scripts.
- Exclude private keys and environment files from Git.
- Restrict PostgreSQL network access.
- Monitor Airflow task execution and failures.
- Review S3 bucket permissions and access policies.
- Keep generated reports and temporary files outside source control.

## 12. Current Implementation and Future Improvements

### Implemented

- AWS EC2 Ubuntu environment.
- Dedicated Amazon S3 bucket.
- Raw Spotify CSV datasets stored in S3.
- PostgreSQL database and relational analytics.
- Apache Airflow DAG with five Python tasks.
- PostgreSQL Airflow connection configuration.
- Database connection-testing script.
- XCom-based task result collection.
- Consolidated analytics report generation.

### Future Improvements

- Automate ingestion of raw CSV files from S3 into PostgreSQL.
- Add an Airflow S3 sensor or ingestion task.
- Upload generated analytics reports to Amazon S3.
- Implement data validation and quality checks.
- Add retries, failure notifications, and monitoring.
- Add automated testing.
- Introduce CloudWatch monitoring and operational alerts.
- Build a dashboard for Spotify analytics.

## 13. Author

**Oladosu Kolapo Kunle**

GitHub: https://github.com/husmankol-cloud

Project: Spotify AWS Data Engineering Pipeline

---

*This project demonstrates practical experience with AWS infrastructure, Linux administration, Python automation, PostgreSQL, Apache Airflow, cloud storage, and data engineering workflows.*
