from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

import pandas as pd

from datetime import datetime, timedelta
import os


INPUT_DIR = "/home/nismo/data/"
ARCHIVE_DIR = "/home/nismo/data/archive"






default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 4),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

with DAG(
    dag_id="git_test_dag",
    default_args=default_args,
    description='Testing',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Functions
    def print_start():
        print("Starting ETL process")


    def test_db():
        pg_hook = PostgresHook(postgres_conn_id='DO_PostGres')
        connection = pg_hook.get_conn()
        cur = connection.cursor()
        cur.execute("SELECT * FROM testing")
        print(cur.fetchall())
        cur.close()
        connection.close

    @task
    def check_sftp_for_file():
        sftp_hook = SFTPHook(ssh_conn_id="Ubuntu_Dev_SFTP")
        pg_hook = PostgresHook(postgres_conn_id='DO_PostGres')

        items = sftp_hook.list_directory(INPUT_DIR)

        for item in items:
            if sftp_hook.isfile(INPUT_DIR + item):

                remote_path = os.path.join(INPUT_DIR, items[1])
                archive_path = os.path.join(ARCHIVE_DIR, items[1])
                local_tmp_path = f"/tmp/{items[1]}"

                print(remote_path, archive_path, local_tmp_path)

                sftp_hook.retrieve_file(remote_path, local_tmp_path)

                df = pd.read_csv(local_tmp_path)
                df = df.rename(columns={
                    'col1': 'id',
                    'col2': 'name'
                })

                sftp_hook.get_conn().rename(remote_path, archive_path)

                

                target_fields = df.columns.tolist()
                rows = [tuple(x) for x in df.values]

                pg_hook.insert_rows(
                    table="testing",
                    rows=rows,
                    target_fields=target_fields,
                    replace=True,
                    replace_index="id"
                )
        
        sftp_hook.close_conn()
        

    # Tasks
    start_task = PythonOperator(
            task_id='print_start',
            python_callable=print_start
            )
    
    test_database = PythonOperator(
            task_id='test_database',
            python_callable=test_db
            )

    test_sftp = check_sftp_for_file()


    start_task >> test_database >> test_sftp