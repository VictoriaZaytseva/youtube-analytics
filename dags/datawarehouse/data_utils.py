from airflow.providers.postgres.hooks.postgres import PostgresHook
from pyscopg2.extras import RealDictCursor

table = "yt_api"

def get_conn_cursor():
    pg_hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cursor

def close_conn_cursor(conn, cursor):
    cursor.close()
    conn.close()

def create_schema(schema):
    conn, cursor = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    cursor.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cursor)

def create_table(schema):
    conn, cursor = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" VARCHAR(20) NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comment_Count" INT
        );"""
    else:
        table_sql = f"""CREATE TABLE IF NOT EXISTS {schema}.{table} (
            "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
            "Video_Title" TEXT NOT NULL,
            "Upload_Date" TIMESTAMP NOT NULL,
            "Duration" TIME NOT NULL,
            "Video_Views" INT,
            "Likes_Count" INT,
            "Comment_Count" INT
        );"""
    cursor.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cursor)        

def get_video_ids(cur, schema):
    cur.execute(f"SELECT \"Video_ID\" FROM {schema}.{table};")
    ids = cur.fetchall()

    video_ids = [row["Video_ID"] for row in ids]

    return video_ids
