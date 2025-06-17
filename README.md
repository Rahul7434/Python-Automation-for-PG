# Python-Automation-for-PG

1. Top Tables
```
import psycopg2
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Database connection details
DB_CONFIG = {
    "host": "hostname",
    "port": "port",
    "user": "username",
    "password": "password",  # Ensure you provide the password
    "dbname": "db_name"
}

# Query to retrieve table size details
QUERY = """
SELECT
    schemaname AS table_schema,
    relname AS table_name,
    pg_size_pretty(pg_total_relation_size(relid)) AS total_size,
    pg_size_pretty(pg_relation_size(relid)) AS data_size,
    pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) AS external_size
FROM
    pg_catalog.pg_statio_user_tables
ORDER BY
    pg_total_relation_size(relid) DESC,
    pg_relation_size(relid) DESC
LIMIT 20;
"""

# Function to fetch data
def fetch_table_sizes():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(QUERY)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows
    except Exception as error:
        print(f"Error: {error}")
        return None

# Function to format data as an HTML table
def format_html_table(rows):
    html = """<html><body><table border="1">
    <tr><th>Schema</th><th>Table</th><th>Total Size</th><th>Data Size</th><th>External Size</th></tr>"""
    
    for row in rows:
        html += f"<tr><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td><td>{row[3]}</td><td>{row[4]}</td></tr>"
    
    html += "</table></body></html>"
    return html

# Function to send email
def send_email(html_content, recipient_email):
    sender_email = "your_email@example.com"
    password = "your_email_password"  # Use environment variables for better security
    
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = recipient_email
    msg["Subject"] = "Database Table Size Report"
    
    msg.attach(MIMEText(html_content, "html"))
    
    try:
        with smtplib.SMTP("smtp.example.com", 587) as server:  # Replace with actual SMTP server
            server.starttls()
            server.login(sender_email, password)
            server.sendmail(sender_email, recipient_email, msg.as_string())
        print("Email sent successfully")
    except Exception as error:
        print(f"Failed to send email: {error}")

# Execution
rows = fetch_table_sizes()
if rows:
    html_table = format_html_table(rows)
    send_email(html_table, "recipient@example.com")  # Replace with the recipient's email
```
3. Dublicate Index
4. Backup History of last 30 days.
5. Alert if no checkpoint occured last 30 min
6. Alert mount point usages grater than threshold.
7. Alert replication lag
8. Alert Max connections
9. Alert blocking
10. long running queries
11. Alert and recommendation to run Vacuum full if table and index bloating exit it's size.
12. vacuum & analyze
13. REINDEXING
14. Backup
15. Invalid Object Checker: Identify orphaned foreign keys, missing indexes on foreign keys, invalid constraints.
16. Automated Restore Validator: Restore backup to test environment and validate with integrity checks.
17. 
