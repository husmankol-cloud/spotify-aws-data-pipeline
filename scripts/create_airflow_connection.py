from getpass import getpass
from airflow.models import Connection
from airflow.settings import Session

password = getpass("Enter PostgreSQL password for airflow_user: ")

session = Session()

try:
    conn = session.query(Connection).filter(
        Connection.conn_id == "spotify_postgres"
    ).first()

    if conn is None:
        conn = Connection(
            conn_id="spotify_postgres",
            conn_type="postgres"
        )

    conn.host = "localhost"
    conn.schema = "spotify_analytics"
    conn.login = "airflow_user"
    conn.password = password
    conn.port = 5432

    session.add(conn)
    session.commit()

    print("Airflow PostgreSQL connection saved successfully!")

finally:
    session.close()
