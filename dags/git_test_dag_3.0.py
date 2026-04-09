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

    @task
    def print_something1():
        print("Something1")

    @task
    def print_something2():
        print("Something2")

    @task
    def print_something3():
        print("Something3")   

    @task
    def print_end():
        print("End")


    
    print_start() >> [print_something1(), print_something2(), print_something3()] >> print_end()


main()