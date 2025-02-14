import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.decorators import dag

import create_tables_sql

PostgresOperator = SQLExecuteQueryOperator

AIRFLOW_AWS_CONN_ID = "aws_default"
AIRFLOW_REDSHIFT_ID = "redshift"

@dag(start_date=pendulum.now(),
     max_active_runs=1)
def create_tables():
    create_artists_table = PostgresOperator(
        task_id='create_artists_table',
        conn_id=AIRFLOW_REDSHIFT_ID,
        sql=create_tables_sql.CREATE_ARTISTS_TABLE_SQL
    )

    create_songplays_table = PostgresOperator(
        task_id='create_songplays_table',
        conn_id=AIRFLOW_REDSHIFT_ID,
        sql=create_tables_sql.CREATE_SONGPLAYS_TABLE_SQL
    )

    create_songs_table = PostgresOperator(
        task_id='create_songs_table',
        conn_id=AIRFLOW_REDSHIFT_ID,
        sql=create_tables_sql.CREATE_SONGS_TABLE_SQL
    )

    create_time_table = PostgresOperator(
        task_id='create_time_table',
        conn_id=AIRFLOW_REDSHIFT_ID,
        sql=create_tables_sql.CREATE_TIME_TABLE_SQL
    )

    create_users_table = PostgresOperator(
        task_id='create_users_table',
        conn_id=AIRFLOW_REDSHIFT_ID,
        sql=create_tables_sql.CREATE_USERS_TABLE_SQL
    )

    create_staging_events_table = PostgresOperator(
        task_id='create_staging_events_table',
        conn_id=AIRFLOW_REDSHIFT_ID,
        sql=create_tables_sql.CREATE_STAGING_EVENTS_TABLE_SQL
    )

    create_staging_songs_table = PostgresOperator(
        task_id='create_staging_songs_table',
        conn_id=AIRFLOW_REDSHIFT_ID,
        sql=create_tables_sql.CREATE_STAGING_SONGS_TABLE_SQL
    )


create_tables_dag = create_tables()
