from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from pendulum import duration
import random 

with DAG(
    dag_id="bash_postgres",
    start_date=datetime(2025, 9, 1),
    schedule='23-26 * * * *',
    # schedule='48-58 * * * *',
    catchup=False,
    description="This is my postgres DAG",
    tags=["data_eng", "1st_dag", "postgres"],
    default_args={"retries": 1},
    dagrun_timeout=duration(minutes=20)
):
    def randomrandom():
        pilihan = random.randint(1,24)
        return f"run_sql_{pilihan}"
    
    branch = BranchPythonOperator(
        task_id='choose_random_sql',
        python_callable=randomrandom
    )

    create_table = PostgresOperator(
        task_id = "create_table",
        postgres_conn_id='my_simple_postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS siswa (
                id SERIAL PRIMARY KEY,
                nama_siswa VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS mapel (
                id SERIAL PRIMARY KEY,
                nama_mapel VARCHAR(50) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS nilai (
                id SERIAL PRIMARY KEY,
                id_siswa INT NOT NULL,
                id_mapel INT NOT NULL,
                nilai_siswa INT NOT NULL CHECK (nilai_siswa BETWEEN 0 AND 100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT fk_siswa FOREIGN KEY (id_siswa) REFERENCES siswa(id) ON DELETE CASCADE,
                CONSTRAINT fk_mapel FOREIGN KEY (id_mapel) REFERENCES mapel(id) ON DELETE CASCADE
            );

            INSERT INTO siswa (id, nama_siswa, created_at) VALUES
                (1, 'Andi', CURRENT_TIMESTAMP),
                (2, 'Budi', CURRENT_TIMESTAMP),
                (3, 'Citra', CURRENT_TIMESTAMP),
                (4, 'Dedi', CURRENT_TIMESTAMP),
                (5, 'Eka',  CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO NOTHING;

            INSERT INTO mapel (id, nama_mapel, created_at) VALUES
                (1, 'Matematika', CURRENT_TIMESTAMP),
                (2, 'Fisika',     CURRENT_TIMESTAMP),
                (3, 'Kimia',      CURRENT_TIMESTAMP),
                (4, 'Biologi',    CURRENT_TIMESTAMP),
                (5, 'Inggris',    CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO NOTHING;

        """
    )

    # insert_data = PostgresOperator(
    #     task_id="insert_data",
    #     postgres_conn_id="my_simple_postgres",

    #     sql="""
    #         INSERT INTO siswa (id, nama_siswa, created_at) VALUES (1, 'Andi', '2023-10-01 08:00:00'),
    #                                                                 (2, 'Budi', '2023-10-01 09:00:00')

    #     """
    # )

    run_sql_1 = PostgresOperator(
        task_id="run_sql_1",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (1, 1, 85, CURRENT_TIMESTAMP);

        """
    )

    run_sql_2 = PostgresOperator(
        task_id="run_sql_2",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (1, 2, 90, CURRENT_TIMESTAMP);

        """
    )

    run_sql_3 = PostgresOperator(
        task_id="run_sql_3",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (2, 1, 78, CURRENT_TIMESTAMP);

        """
    )

    run_sql_4 = PostgresOperator(
        task_id="run_sql_4",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (2, 3, 88, CURRENT_TIMESTAMP);

        """
    )

    run_sql_5 = PostgresOperator(
        task_id="run_sql_5",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (3, 4, 92, CURRENT_TIMESTAMP);

        """
    )

    run_sql_6 = PostgresOperator(
        task_id="run_sql_6",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (3, 5, 80, CURRENT_TIMESTAMP);

        """
    )

    run_sql_7 = PostgresOperator(
        task_id="run_sql_7",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (4, 1, 70, CURRENT_TIMESTAMP);

        """
    )

    run_sql_8 = PostgresOperator(
        task_id="run_sql_8",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (4, 3, 75, CURRENT_TIMESTAMP);

        """
    )

    run_sql_9 = PostgresOperator(
        task_id="run_sql_9",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (5, 2, 95, CURRENT_TIMESTAMP);

        """
    )

    run_sql_10 = PostgresOperator(
        task_id="run_sql_10",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (5, 5, 85, CURRENT_TIMESTAMP);

        """
    )

    run_sql_11 = PostgresOperator(
        task_id="run_sql_11",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (1, 3, 88, CURRENT_TIMESTAMP);

        """
    )

    run_sql_12 = PostgresOperator(
        task_id="run_sql_12",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (1, 4, 91, CURRENT_TIMESTAMP);

        """
    )

    run_sql_13 = PostgresOperator(
        task_id="run_sql_13",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (2, 2, 76, CURRENT_TIMESTAMP);

        """
    )

    run_sql_14 = PostgresOperator(
        task_id="run_sql_14",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (2, 4, 84, CURRENT_TIMESTAMP);

        """
    )

    run_sql_15 = PostgresOperator(
        task_id="run_sql_15",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (3, 1, 89, CURRENT_TIMESTAMP);

        """
    )

    run_sql_16 = PostgresOperator(
        task_id="run_sql_16",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (3, 3, 93, CURRENT_TIMESTAMP);

        """
    )

    run_sql_17 = PostgresOperator(
        task_id="run_sql_17",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (4, 2, 67, CURRENT_TIMESTAMP);

        """
    )

    run_sql_18 = PostgresOperator(
        task_id="run_sql_18",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (4, 4, 72, CURRENT_TIMESTAMP);

        """
    )

    run_sql_19 = PostgresOperator(
        task_id="run_sql_19",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (5, 1, 90, CURRENT_TIMESTAMP);

        """
    )

    run_sql_20 = PostgresOperator(
        task_id="run_sql_20",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (5, 3, 88, CURRENT_TIMESTAMP);

        """
    )

    run_sql_21 = PostgresOperator(
        task_id="run_sql_21",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (1, 5, 87, CURRENT_TIMESTAMP);

        """
    )

    run_sql_22 = PostgresOperator(
        task_id="run_sql_22",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (2, 5, 81, CURRENT_TIMESTAMP);

        """
    )

    run_sql_23 = PostgresOperator(
        task_id="run_sql_23",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (3, 2, 79, CURRENT_TIMESTAMP);

        """
    )

    run_sql_24 = PostgresOperator(
        task_id="run_sql_24",
        postgres_conn_id="my_simple_postgres",

        sql="""
            INSERT INTO nilai (id_siswa, id_mapel, nilai_siswa, created_at) VALUES (4, 5, 73, CURRENT_TIMESTAMP);

        """
    )

    # create_table >>  insert_data
    create_table >> branch >> [run_sql_1, run_sql_2, run_sql_3, run_sql_4, run_sql_5, run_sql_6, run_sql_7, run_sql_8, run_sql_9, run_sql_10, run_sql_11, run_sql_12,
                                run_sql_13, run_sql_14, run_sql_15, run_sql_16, run_sql_17, run_sql_18, run_sql_19, run_sql_20, run_sql_21, run_sql_22, run_sql_23, run_sql_24] 
    