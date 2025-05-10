import os
import psycopg2

def query():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
    with conn.cursor() as cur:
        cur.execute(

            """ 
            CREATE TABLE IF NOT EXISTS tabela_rh(
            )
            """
        )