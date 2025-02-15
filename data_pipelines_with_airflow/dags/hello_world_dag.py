import logging
import pendulum

from airflow.decorators import dag, task


# @dag decorates the greet_task to denote it's the main function
@dag(
    start_date=pendulum.now()
)
def hello_world():
    # @task decorates the re-usable hello_world_task - it can be called as often as needed in the DAG
    @task
    def hello_world_task():
        logging.info("Hello World!")

    # hello_world represents a discrete invocation of the hello_world_task
    hello_world = hello_world_task()


# hello_world represents the invocation of the hello_world_dag
hello_world = hello_world()
