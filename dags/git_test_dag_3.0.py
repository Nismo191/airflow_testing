from airflow.sdk import dag, task


from datetime import datetime, timedelta


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}




@dag(
    "3.0_test",
    default_args = default_args,
    description = "A 3.0 test",
    schedule = timedelta(days=1),
    start_date = datetime(2026, 1, 1),
    catchup = False,
    tags = ["testing"]
)
def main():
    
    @task
    def print_start():
        print("Starting")


    
    print_task = print_start()


main()