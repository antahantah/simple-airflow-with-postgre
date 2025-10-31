from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from google.cloud import bigquery
from datetime import datetime
import json
import os

# KONFIGURASI POSTGRES
POSTGRES_CONN_ID = "my_simple_postgres"

# KONFIGURASI BIGQUERY
CREDENTIALS_PATH = '/opt/airflow/gcp-key.json'

BIGQUERY_PROJECT = 'jcdeah-006'
BIGQUERY_DATASET = 'adit_nilaiSiswa_capstone3'
BIGQUERY_TABLE = 'siswa'

# KONFIGURASI SCHEMA TABLE
SISWA_SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("nama_siswa", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
]

MAPEL_SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("nama_mapel", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
]

NILAI_SCHEMA = [
    bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("id_siswa", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("id_mapel", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("nilai_siswa", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
]

# SECTION EXTRACT SISWA
def extract_siswa():
    """
    Extract data dari tabel siswa dan disimpan ke .json
    """
    print("\n" + "="*30)
    print("EXTRACTING FROM SISWA TABLE")
    print("="*30)
    
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # query
    cursor.execute("SELECT * FROM siswa")

    # cursor.execute("""
    #     SELECT * FROM siswa
    #     WHERE DATE(created_at) = CURRENT_DATE - INTERVAL '1 days'
    # """)
    
    # hasil dari extraction
    column_names = [desc[0] for desc in cursor.description] #ambil header column (id, nama, dsb)
    rows = cursor.fetchall() # ambil semua data
    
    # Print hasil
    print(f"Columns: {column_names}")
    print(f"Rows found: {len(rows)}\n")

    # Close connection demi penghematan resource
    cursor.close()
    conn.close()

    #------------------------------------------------------------

    # Konversi data dari Postgres ke struktur JSON
    extr_data = []
    for row_extr in rows:
        row_extr_dict = {}
        for i, col_extr in enumerate(column_names):
            value = row_extr[i]

            # Convert datetime to string (JSON can't store datetime objects)
            if hasattr(value, 'isoformat'):
                value = value.isoformat()
            
            row_extr_dict[col_extr] = value
        
        extr_data.append(row_extr_dict)

    print(f"Converted {len(extr_data)} rows to JSON format")

    # Simpan ke file json pada folder /tmp
    filepath = '/tmp/siswa_data.json'
    
    with open(filepath, 'w') as f:
        json.dump(extr_data, f, indent=2)
    
    print(f"File saved: {filepath}")
    
    # Show file size
    file_size = os.path.getsize(filepath)
    print(f"File size: {file_size} bytes ({file_size/1024:.2f} KB)")

    return filepath

# SECTION BIGQUERY SISWA
def load_siswa():
    """
    Baca JSON siswa dan export ke BIGQUERY

    """
    print("\n" + "="*30)
    print("LOAD SISWA TO BIGQUERY")
    print("="*30)

    # Baca JSON file yang telah dibuat sebelumnya
    filepath = '/tmp/siswa_data.json'

    with open(filepath, 'r') as f:
        data = json.load(f)

    print(f"File loaded: {filepath}")
    print(f"Rows in file: {len(data)}")

    # Connect ke BigQuery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_PATH
    client = bigquery.Client(project=BIGQUERY_PROJECT)

    # Persiapkan table (schema, column, partition, dsb) pada bigquery
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.siswa"
    table = bigquery.Table(table_id, schema=SISWA_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='created_at',
    )

    # Create table if not exists
    try:
        client.create_table(table)
        print("Table created with partitioning")
    except:
        print("Table already exists")

    print(f"Target table: {table_id}")

    job_config = bigquery.LoadJobConfig(
        schema=SISWA_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    # Load data ke bigquery
    print(f"Uploading {len(data)} rows...")

    job = client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()  # Wait for completion

    return len(data)


# SECTION EXTRACT MAPEL
def extract_mapel():
    """
    Extract data dari tabel mapel dan disimpan ke .json
    """
    print("\n" + "="*30)
    print("EXTRACTING FROM MAPEL TABLE")
    print("="*30)
    
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # query
    cursor.execute("""
        SELECT * FROM mapel
        WHERE DATE(created_at) = CURRENT_DATE - INTERVAL '1 days'
    """)
    
    # hasil dari extraction
    column_names = [desc[0] for desc in cursor.description] #ambil header column (id, nama, dsb)
    rows = cursor.fetchall() # ambil semua data
    
    # Print hasil
    print(f"Columns: {column_names}")
    print(f"Rows found: {len(rows)}\n")

    # Close connection demi penghematan resource
    cursor.close()
    conn.close()

    #------------------------------------------------------------

    # Konversi data dari Postgres ke struktur JSON
    extr_data = []
    for row_extr in rows:
        row_extr_dict = {}
        for i, col_extr in enumerate(column_names):
            value = row_extr[i]

            # Convert datetime to string (JSON can't store datetime objects)
            if hasattr(value, 'isoformat'):
                value = value.isoformat()
            
            row_extr_dict[col_extr] = value
        
        extr_data.append(row_extr_dict)

    print(f"Converted {len(extr_data)} rows to JSON format")

    # Simpan ke file json pada folder /tmp
    filepath = '/tmp/mapel_data.json'
    
    with open(filepath, 'w') as f:
        json.dump(extr_data, f, indent=2)
    
    print(f"File saved: {filepath}")
    
    # Show file size
    file_size = os.path.getsize(filepath)
    print(f"File size: {file_size} bytes ({file_size/1024:.2f} KB)")

    return filepath

# SECTION BIGQUERY MAPEL
def load_mapel():
    """
    Baca JSON mapel dan export ke BIGQUERY

    """
    print("\n" + "="*30)
    print("LOAD MAPEL TO BIGQUERY")
    print("="*30)

    # Baca JSON file yang telah dibuat sebelumnya
    filepath = '/tmp/mapel_data.json'

    with open(filepath, 'r') as f:
        data = json.load(f)

    print(f"File loaded: {filepath}")
    print(f"Rows in file: {len(data)}")

    # Connect ke BigQuery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_PATH
    client = bigquery.Client(project=BIGQUERY_PROJECT)

    # Persiapkan table (schema, column, partition, dsb) pada bigquery
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.mapel"
    table = bigquery.Table(table_id, schema=MAPEL_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='created_at',
    )

    # Create table if not exists
    try:
        client.create_table(table)
        print("Table created with partitioning")
    except:
        print("Table already exists")

    print(f"Target table: {table_id}")

    job_config = bigquery.LoadJobConfig(
        schema=MAPEL_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    # Load data ke bigquery
    print(f"Uploading {len(data)} rows...")

    job = client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()  # Wait for completion

    return len(data)

# SECTION EXTRACT NILAI
def extract_nilai():
    """
    Extract data dari tabel nilai dan disimpan ke .json
    """
    print("\n" + "="*30)
    print("EXTRACTING FROM NILAI TABLE")
    print("="*30)
    
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # query
    cursor.execute("""
        SELECT * FROM nilai
        WHERE DATE(created_at) = CURRENT_DATE - INTERVAL '1 days'
    """)
    
    # hasil dari extraction
    column_names = [desc[0] for desc in cursor.description] #ambil header column (id, nama, dsb)
    rows = cursor.fetchall() # ambil semua data
    
    # Print hasil
    print(f"Columns: {column_names}")
    print(f"Rows found: {len(rows)}\n")

    # Close connection demi penghematan resource
    cursor.close()
    conn.close()

    #------------------------------------------------------------

    # Konversi data dari Postgres ke struktur JSON
    extr_data = []
    for row_extr in rows:
        row_extr_dict = {}
        for i, col_extr in enumerate(column_names):
            value = row_extr[i]

            # Convert datetime to string (JSON can't store datetime objects)
            if hasattr(value, 'isoformat'):
                value = value.isoformat()
            
            row_extr_dict[col_extr] = value
        
        extr_data.append(row_extr_dict)

    print(f"Converted {len(extr_data)} rows to JSON format")

    # Simpan ke file json pada folder /tmp
    filepath = '/tmp/nilai_data.json'
    
    with open(filepath, 'w') as f:
        json.dump(extr_data, f, indent=2)
    
    print(f"File saved: {filepath}")
    
    # Show file size
    file_size = os.path.getsize(filepath)
    print(f"File size: {file_size} bytes ({file_size/1024:.2f} KB)")

    return filepath

# SECTION BIGQUERY NILAI
def load_nilai():
    """
    Baca JSON nilai dan export ke BIGQUERY

    """
    print("\n" + "="*30)
    print("LOAD NILAI TO BIGQUERY")
    print("="*30)

    # Baca JSON file yang telah dibuat sebelumnya
    filepath = '/tmp/nilai_data.json'

    with open(filepath, 'r') as f:
        data = json.load(f)

    print(f"File loaded: {filepath}")
    print(f"Rows in file: {len(data)}")

    # Connect ke BigQuery
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = CREDENTIALS_PATH
    client = bigquery.Client(project=BIGQUERY_PROJECT)

    # Persiapkan table (schema, column, partition, dsb) pada bigquery
    table_id = f"{BIGQUERY_PROJECT}.{BIGQUERY_DATASET}.nilai"
    table = bigquery.Table(table_id, schema=NILAI_SCHEMA)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field='created_at',
    )

    # Create table if not exists
    try:
        client.create_table(table)
        print("Table created with partitioning")
    except:
        print("Table already exists")

    print(f"Target table: {table_id}")

    job_config = bigquery.LoadJobConfig(
        schema=NILAI_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )

    # Load data ke bigquery
    print(f"Uploading {len(data)} rows...")

    job = client.load_table_from_json(data, table_id, job_config=job_config)
    job.result()  # Wait for completion

    return len(data)


# Define DAG
with DAG(
    'step_extract_n_load_to_bq',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['bigquery', 'extract', '2nd-dag'],
) as dag:
    
    # SISWA tasks
    task_extract_siswa = PythonOperator(
        task_id='extract_siswa',
        python_callable=extract_siswa,
    )
    
    task_load_siswa = PythonOperator(
        task_id='load_siswa',
        python_callable=load_siswa,
    )

    # MAPEL tasks
    task_extract_mapel = PythonOperator(
        task_id='extract_mapel',
        python_callable=extract_mapel,
    )
    
    task_load_mapel = PythonOperator(
        task_id='load_mapel',
        python_callable=load_mapel,
    )
    
    # NILAI tasks
    task_extract_nilai = PythonOperator(
        task_id='extract_nilai',
        python_callable=extract_nilai,
    )
    
    task_load_nilai = PythonOperator(
        task_id='load_nilai',
        python_callable=load_nilai,
    )

    # Define dependency: Extract must complete before load
    task_extract_siswa >> task_load_siswa
    task_extract_mapel >> task_load_mapel
    task_extract_nilai >> task_load_nilai