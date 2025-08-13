import os, csv, time
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

def get_conn():
    load_dotenv()
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "etl_project"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASS", ""),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    )

DDL = """
CREATE TABLE IF NOT EXISTS customers (
    id   INT PRIMARY KEY,
    name TEXT NOT NULL,
    city TEXT NOT NULL
);
"""

UPSERT_SQL = """
INSERT INTO customers (id, name, city)
VALUES %s
ON CONFLICT (id) DO NOTHING;
"""

def main():
    csv_path = os.path.join("data", "customers.csv")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing CSV at {csv_path}")

    # Read rows from CSV
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [(int(r["id"]), r["name"], r["city"]) for r in reader]

    if not rows:
        print("No rows found in CSV; nothing to load.")
        return

    t0 = time.time()
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            # Ensure table exists
            cur.execute(DDL)
            # Batch insert with ON CONFLICT DO NOTHING (idempotent)
            execute_values(cur, UPSERT_SQL, rows)
            # Row count for logging
            cur.execute("SELECT COUNT(*) FROM customers;")
            total = cur.fetchone()[0]

    conn.close()
    dt = time.time() - t0
    print(f"Loaded {len(rows)} rows from {csv_path} in {dt:.2f}s")
    print(f"customers total rows now: {total}")

if __name__ == "__main__":
    main()
