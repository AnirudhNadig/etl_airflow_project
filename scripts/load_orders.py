import os, csv, time
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

def get_conn():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME","etl_project"),
        user=os.getenv("DB_USER","postgres"),
        password=os.getenv("DB_PASS",""),
        host=os.getenv("DB_HOST","localhost"),
        port=os.getenv("DB_PORT","5432"),
    )

DDL = """
CREATE TABLE IF NOT EXISTS orders (
  id          INT PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(id),
  amount      NUMERIC(12,2) NOT NULL,
  ts          TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_ts ON orders(ts);
"""

UPSERT = """
INSERT INTO orders (id, customer_id, amount, ts)
VALUES %s
ON CONFLICT (id) DO NOTHING;
"""

def main():
    csv_path = Path("data/orders.csv")
    rows = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append((
                int(row["id"]),
                int(row["customer_id"]),
                float(row["amount"]),
                row["ts"],  # let Postgres parse ISO8601
            ))
    t0 = time.time()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(DDL)
        execute_values(cur, UPSERT, rows)
        cur.execute("SELECT COUNT(*) FROM orders;")
        total = cur.fetchone()[0]
    print(f"Loaded {len(rows)} orders in {time.time()-t0:.2f}s; total now {total}")

if __name__ == "__main__":
    main()
