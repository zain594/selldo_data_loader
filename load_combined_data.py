import os
import psycopg2

# Read DB connection details from env vars
SELLDO_DB = os.environ.get("SELLDO_DB")
REPORTING_DB = os.environ.get("REPORTING_DB")

def main():
    # Connect to Sell.Do DB
    sell_conn = psycopg2.connect(SELLDO_DB)
    sell_cur = sell_conn.cursor()

    # Connect to your reporting DB
    report_conn = psycopg2.connect(REPORTING_DB)
    report_cur = report_conn.cursor()

    # Example query - adjust as needed
    sell_cur.execute("""
        SELECT l.id AS lead_id, l.name AS lead_name
        FROM reporting_leads l
        LIMIT 10;
    """)
    data = sell_cur.fetchall()

    # Clear old data
    report_cur.execute("DELETE FROM combined_data;")

    # Insert new data
    for row in data:
        report_cur.execute("""
            INSERT INTO combined_data (lead_id, lead_name)
            VALUES (%s, %s)
        """, row)

    report_conn.commit()

    sell_cur.close()
    sell_conn.close()
    report_cur.close()
    report_conn.close()

if __name__ == "__main__":
    main()
