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

    try:
        sell_conn = psycopg2.connect(SELLDO_DB)
        sell_cur = sell_conn.cursor()

        report_conn = psycopg2.connect(REPORTING_DB)
        report_cur = report_conn.cursor()

        # Example query
        sell_cur.execute("""
            SELECT id, name FROM reporting_leads LIMIT 10;
        """)
        data = sell_cur.fetchall()

        # Clear and insert
        report_cur.execute("DELETE FROM combined_data;")
        for row in data:
            report_cur.execute("""
                INSERT INTO combined_data (lead_id, lead_name)
                VALUES (%s, %s)
            """, row)

        report_conn.commit()

        # Close connections
        sell_cur.close()
        sell_conn.close()
        report_cur.close()
        report_conn.close()

        return {"status": "success", "rows_inserted": len(data)}

    except Exception as e:
        return {"status": "error", "detail": str(e)}
