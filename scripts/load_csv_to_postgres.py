import os
import pandas as pd
import psycopg2

DB_NAME = os.getenv("DB_NAME", "etl_project")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def main():
    df = pd.read_csv(os.path.join("data", "sample_customers.csv"))
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS,
        host=DB_HOST, port=DB_PORT
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id   INT PRIMARY KEY,
            name TEXT,
            city TEXT
        );
    """)
    for row in df.itertuples(index=False):
        cur.execute("""
            INSERT INTO customers (id, name, city)
            VALUES (%s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, (int(row.id), row.name, row.city))
    conn.commit()
    cur.close(); conn.close()
    print("Loaded sample_customers.csv into customers")

if __name__ == "__main__":
    main()
