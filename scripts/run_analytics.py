import os, csv
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

OUT = Path("out"); OUT.mkdir(exist_ok=True)

def run_to_csv(query: str, out_path: Path):
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASS"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
    )
    with conn, conn.cursor() as cur:
        cur.execute(query)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols); w.writerows(rows)
    print(f"Wrote {out_path} ({len(rows)} rows)")

if __name__ == "__main__":
    run_to_csv("""
      SELECT c.city, COUNT(o.id) AS orders, ROUND(SUM(o.amount),2) AS total_amount
      FROM customers c LEFT JOIN orders o ON o.customer_id = c.id
      GROUP BY c.city ORDER BY total_amount DESC NULLS LAST;
    """, OUT / "orders_per_city.csv")

    run_to_csv("""
      SELECT c.id, c.name, c.city, ROUND(SUM(o.amount),2) AS total_spend, COUNT(o.id) AS order_count
      FROM customers c JOIN orders o ON o.customer_id = c.id
      GROUP BY c.id, c.name, c.city ORDER BY total_spend DESC LIMIT 10;
    """, OUT / "top_customers.csv")

    run_to_csv("""
      SELECT date_trunc('month', o.ts) AS month, ROUND(SUM(o.amount),2) AS revenue
      FROM orders o GROUP BY 1 ORDER BY 1;
    """, OUT / "monthly_revenue.csv")
