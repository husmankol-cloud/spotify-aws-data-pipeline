from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Retrieve our saved Airflow connection
connection = BaseHook.get_connection("spotify_postgres")

# Create a PostgreSQL connection using Airflow
postgres = PostgresHook(postgres_conn_id="spotify_postgres")

# Run a read-only query
result = postgres.get_first("SELECT COUNT(*) FROM songs;")

print("PostgreSQL connection successful!")
print(f"Number of songs in database: {result[0]}")
