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
```

```
5. Backup History of last 30 days
```
```
7. Alert if no checkpoint occured last 30 min
```
```
9. Alert mount point usages grater than threshold
```
```
11. Alert replication lag
```
```
13. Alert Max connections
```
```
15. Alert blocking
```
```
17. long running queries
```
```
19. Alert and recommendation to run Vacuum full if table and index bloating exit it's size
```
```
21. vacuum & analyze
```
```
23. REINDEXING
```
import psycopg2
 
# The SQL query to analyze index bloat
QUERY = """
WITH index_stats AS (
    SELECT
        n.nspname AS schema_name,
        c.relname AS table_name,
        i.relname AS index_name,
        pg_relation_size(i.oid) AS index_size_bytes,
        ui.idx_scan AS index_scans,
        t.n_live_tup AS live_tuples,
        t.n_dead_tup AS dead_tuples,
        pg_relation_size(c.oid) AS table_size_bytes,
        c.reltuples AS reltuples,
        idx.indisprimary,
        idx.indisvalid,
        con.contype
    FROM
        pg_stat_user_indexes ui
    JOIN pg_index idx ON ui.indexrelid = idx.indexrelid
    JOIN pg_class i ON i.oid = ui.indexrelid
    JOIN pg_class c ON c.oid = idx.indrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_stat_user_tables t ON t.relid = c.oid
    LEFT JOIN pg_constraint con ON con.conindid = idx.indexrelid
    WHERE
        idx.indisvalid = true
),
bloat_estimation AS (
    SELECT
        schema_name,
        index_name,
        pg_size_pretty(index_size_bytes) AS index_size,
        (index_size_bytes - (reltuples * 16)) / 1000000 AS index_bloat_mb,
        CASE
            WHEN index_size_bytes = 0 THEN 0
            ELSE (index_size_bytes - (reltuples * 16))::float / NULLIF(index_size_bytes, 0)
        END AS bloat_ratio
    FROM
        index_stats
    WHERE
        NOT indisprimary AND (contype IS DISTINCT FROM 'f')
)
SELECT
    schema_name,
    index_name,
    index_size,
    index_bloat_mb,
    ROUND((bloat_ratio * 100)::numeric, 2) AS bloat_percentage
FROM
    bloat_estimation
WHERE
    index_bloat_mb > 0
ORDER BY
    index_bloat_mb DESC;
"""
 
# Database connection details
DB_HOST = "bpay-pgflexi-pr-ats.postgres.database.azure.com"
DB_PORT = "5432"
#DB_NAME = "postgres"
DB_USER = "psqladmin"
DB_PASSWORD = "P@ssw0rd@5432@2023"
 
# List to store bloated indexes
index_list = []
db_list = ['atsdb']
# Connect to PostgreSQL server
for db in db_list:
        try:
                conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=db, user=DB_USER, password=DB_PASSWORD)
                conn.autocommit = True  # Ensure queries execute immediately
                cursor = conn.cursor()
 
                # Execute query to fetch index bloat details
                cursor.execute(QUERY)
                data = cursor.fetchall()
 
                # Extract indexes with wasted space
                for row in data:
                        if row[4] > 30.00:
                                index_list.append(row[1])
                '''
                # Set PostgreSQL parameters for optimized reindexing
                cursor.execute("SET max_parallel_workers_per_gather = 8;")
                cursor.execute("SET maintenance_work_mem = '20GB';")
                print("Database parameters set: max_parallel_workers_per_gather = 8, maintenance_work_mem = 20GB")
 
                # Loop through indexes and reindex them concurrently
                for index in index_list:
                        try:
                                query = f"REINDEX INDEX CONCURRENTLY {index};"
                                print(f"Reindexing: {index}")
                                cursor.execute(query)
                                print(f"Successfully reindexed: {index}")
                        except Exception as e:
                                print(f"Error reindexing {index}: {e}")
 
                # Reindexing of SYSTEM Catalogs
                print("Reindexing SYSTEM")
                cursor.execute(f"REINDEX SYSTEM {db};")
                print("Successfully reindexed SYSTEM")
 
                # Close the database connection
                cursor.close()
                conn.close()
                print("Database connection closed.")
                '''
                print(f"DB NAME: {db}")
                print(index_list)
        except Exception as db_error:
                print(f"Database connection failed: {db_error}")
```
25. Backup
```
```
27. Invalid Object Checker: Identify orphaned foreign keys, missing indexes on foreign keys, invalid constraints
```
```
29. Automated Restore Validator: Restore backup to test environment and validate with integrity checks
```
```
31. 
