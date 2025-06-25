from fastapi import FastAPI
import psycopg2
import os

app = FastAPI()

@app.get("/")
def healthcheck():
    return {"status": "ok"}

@app.post("/run-loader")
def run_loader():
    SELLDO_DB = os.environ.get("SELLDO_DB")
    REPORTING_DB = os.environ.get("REPORTING_DB")

    if not SELLDO_DB or not REPORTING_DB:
        return {"status": "error", "detail": "Environment variables not set properly"}

    try:
        print("Connecting to Sell.Do DB...")
        sell_conn = psycopg2.connect(SELLDO_DB)
        sell_cur = sell_conn.cursor()

        print("Connecting to Reporting DB...")
        report_conn = psycopg2.connect(REPORTING_DB)
        report_cur = report_conn.cursor()

        print("Fetching data from Sell.Do DB...")
        sell_cur.execute("""
            SELECT id, name 
            FROM reporting_leads 
            LIMIT 10;
        """)
        data = sell_cur.fetchall()
        print(f"Fetched {len(data)} rows")

        print("Ensuring combined_data table exists...")
        report_cur.execute("""
            CREATE TABLE IF NOT EXISTS combined_data (
                lead_id BIGINT,
                lead_name TEXT
            );
        """)

        print("Clearing existing data...")
        report_cur.execute("DELETE FROM combined_data;")

        print("Inserting data into combined_data...")
        for row in data:
            report_cur.execute("""
                INSERT INTO combined_data (lead_id, lead_name)
                VALUES (%s, %s)
            """, row)

        report_conn.commit()
        print("Insert complete")

        # Cleanup
        sell_cur.close()
        sell_conn.close()
        report_cur.close()
        report_conn.close()

        return {"status": "success", "rows_inserted": len(data)}

    except Exception as e:
        print(f"Error: {e}")
        return {"status": "error", "detail": str(e)}
