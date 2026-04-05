from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta


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
        pg_hook = PostgresHook(postgres_conn_id='DO_PostGres', schema='airflow_db_connection')
        connection = pg_hook.get_conn()
        cur = connection.curson()
        cur.execute("SELECT * FROM testing")
        print(cur.fetchall())
        cur.close()
        connection.close



    # Tasks
    start_task = PythonOperator(
            task_id='print_start',
            python_callable=print_start
            )
    
    test_database = PythonOperator(
            task_id='test_database',
            python_callable=test_db
            )



    start_task >> test_database