
import logging
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# --------------------------------------------------
# PROJECT SETTINGS
# --------------------------------------------------

POSTGRES_CONNECTION_ID = "spotify_postgres"

REPORT_DIRECTORY = Path.home() / "spotify-aws-data-pipeline" / "reports"

REPORT_FILE = REPORT_DIRECTORY / "spotify_analytics_report.txt"


# --------------------------------------------------
# HELPER FUNCTION: CONNECT TO POSTGRESQL
# --------------------------------------------------

def get_postgres():
    """
    Create a connection to our PostgreSQL database
    using the Airflow connection we configured.
    """

    return PostgresHook(
        postgres_conn_id=POSTGRES_CONNECTION_ID
    )


# --------------------------------------------------
# TASK 1: MOST-STREAMED SONGS
# --------------------------------------------------

def get_most_streamed_songs():

    postgres = get_postgres()

    sql = """
        SELECT
            songs.track_name,
            songs.artists,
            COUNT(streams.stream_id) AS total_streams
        FROM streams
        JOIN songs
            ON streams.track_id = songs.track_id
        GROUP BY
            songs.track_id,
            songs.track_name,
            songs.artists
        ORDER BY total_streams DESC;
    """

    results = postgres.get_records(sql)

    logging.info("MOST STREAMED SONGS REPORT")

    for row in results:
        logging.info(
            "Song: %s | Artist: %s | Streams: %s",
            row[0],
            row[1],
            row[2]
        )

    return results


# --------------------------------------------------
# TASK 2: STREAMS BY COUNTRY
# --------------------------------------------------

def get_streams_by_country():

    postgres = get_postgres()

    sql = """
        SELECT
            users.user_country,
            COUNT(streams.stream_id) AS total_streams
        FROM streams
        JOIN users
            ON streams.user_id = users.user_id
        GROUP BY users.user_country
        ORDER BY total_streams DESC;
    """

    results = postgres.get_records(sql)

    logging.info("STREAMS BY COUNTRY REPORT")

    for row in results:
        logging.info(
            "Country: %s | Streams: %s",
            row[0],
            row[1]
        )

    return results


# --------------------------------------------------
# TASK 3: STREAMS BY USER
# --------------------------------------------------

def get_streams_by_user():

    postgres = get_postgres()

    sql = """
        SELECT
            users.user_name,
            COUNT(streams.stream_id) AS total_streams
        FROM streams
        JOIN users
            ON streams.user_id = users.user_id
        GROUP BY
            users.user_id,
            users.user_name
        ORDER BY total_streams DESC;
    """

    results = postgres.get_records(sql)

    logging.info("STREAMS BY USER REPORT")

    for row in results:
        logging.info(
            "User: %s | Streams: %s",
            row[0],
            row[1]
        )

    return results


# --------------------------------------------------
# TASK 4: SONG POPULARITY ANALYSIS
# --------------------------------------------------

def get_song_popularity():

    postgres = get_postgres()

    sql = """
        SELECT
            ROUND(AVG(popularity)::numeric, 2),
            MAX(popularity),
            MIN(popularity),
            COUNT(*)
        FROM songs;
    """

    results = postgres.get_first(sql)

    logging.info("SONG POPULARITY REPORT")

    logging.info(
        "Average popularity: %s | Highest: %s | Lowest: %s | Total songs: %s",
        results[0],
        results[1],
        results[2],
        results[3]
    )

    return [
    str(results[0]),
    results[1],
    results[2],
    results[3],
]

# --------------------------------------------------
# TASK 5: GENERATE FINAL ANALYTICS REPORT
# --------------------------------------------------

def generate_analytics_report(**context):

    ti = context["ti"]

    # Collect results from the four previous tasks

    most_streamed = ti.xcom_pull(
        task_ids="get_most_streamed_songs"
    )

    country_results = ti.xcom_pull(
        task_ids="get_streams_by_country"
    )

    user_results = ti.xcom_pull(
        task_ids="get_streams_by_user"
    )

    popularity_results = ti.xcom_pull(
        task_ids="get_song_popularity"
    )

    # Create the report directory if it does not exist

    REPORT_DIRECTORY.mkdir(
        parents=True,
        exist_ok=True
    )

    # Prepare the report

    with open(REPORT_FILE, "w", encoding="utf-8") as report:

        report.write("SPOTIFY ANALYTICS REPORT\n")
        report.write("=" * 50 + "\n\n")

        report.write("1. MOST STREAMED SONGS\n")
        report.write("-" * 30 + "\n")

        for row in most_streamed:
            report.write(
                f"Song: {row[0]} | Artist: {row[1]} | Streams: {row[2]}\n"
            )

        report.write("\n2. STREAMS BY COUNTRY\n")
        report.write("-" * 30 + "\n")

        for row in country_results:
            report.write(
                f"Country: {row[0]} | Streams: {row[1]}\n"
            )

        report.write("\n3. STREAMS BY USER\n")
        report.write("-" * 30 + "\n")

        for row in user_results:
            report.write(
                f"User: {row[0]} | Streams: {row[1]}\n"
            )

        report.write("\n4. SONG POPULARITY\n")
        report.write("-" * 30 + "\n")

        report.write(
            f"Average popularity: {popularity_results[0]}\n"
        )

        report.write(
            f"Highest popularity: {popularity_results[1]}\n"
        )

        report.write(
            f"Lowest popularity: {popularity_results[2]}\n"
        )

        report.write(
            f"Total songs: {popularity_results[3]}\n"
        )

    logging.info("Analytics report created: %s", REPORT_FILE)

    return str(REPORT_FILE)


# --------------------------------------------------
# DEFINE THE AIRFLOW WORKFLOW
# --------------------------------------------------

with DAG(
    dag_id="spotify_analytics_dag",
    description="Automated Spotify analytics using PostgreSQL",
    start_date=datetime(2026, 11, 17),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "postgresql", "analytics"],
) as dag:

    most_streamed_task = PythonOperator(
        task_id="get_most_streamed_songs",
        python_callable=get_most_streamed_songs,
    )

    country_task = PythonOperator(
        task_id="get_streams_by_country",
        python_callable=get_streams_by_country,
    )

    user_task = PythonOperator(
        task_id="get_streams_by_user",
        python_callable=get_streams_by_user,
    )

    popularity_task = PythonOperator(
        task_id="get_song_popularity",
        python_callable=get_song_popularity,
    )

    report_task = PythonOperator(
        task_id="generate_analytics_report",
        python_callable=generate_analytics_report,
    )

    # Define the order in which Airflow executes the tasks

    [
        most_streamed_task,
        country_task,
        user_task,
        popularity_task,
    ] >> report_task
