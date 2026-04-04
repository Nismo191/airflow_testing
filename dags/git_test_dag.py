from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 26),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}



with DAG(
    'Git_Test_Dag',
    default_args=default_args,
    description='Testing',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Functions
    def print_start():
        print("Starting ETL process")


    def print_e():
        print("Extracting...")

    def print_t():
        print("Transforming...")

    def print_l():
        print("Loading...")


    # Tasks
    start_task = PythonOperator(
            task_id='print_start',
            python_callable=print_start
            )

    e_task = PythonOperator(
            task_id='Extract',
            python_callable=print_e
            )

    t_task = PythonOperator(
            task_id='Transform',
            python_callable=print_t
            )

    l_task = PythonOperator(
            task_id='Load',
            python_callable=print_l
            )

    start_task >> e_task >> t_task >> l_task