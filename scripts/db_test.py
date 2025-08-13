import os
import psycopg2
from dotenv import load_dotenv

def main():
    load_dotenv()
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME", "etl_project"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASS", ""),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            print("DB OK:", cur.fetchone()[0])  # should print 1
    conn.close()

if __name__ == "__main__":
    main()
