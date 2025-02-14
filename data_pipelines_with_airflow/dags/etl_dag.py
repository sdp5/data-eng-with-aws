from datetime import datetime, timedelta

from airflow.operators.empty import EmptyOperator
from operators import StageToRedshiftOperator, LoadFactOperator
from operators import LoadDimensionOperator, DataQualityOperator

from helpers import SqlQueries

from airflow.models import Variable

import logging

from airflow.decorators import dag


AIRFLOW_AWS_CONN_ID = "aws_default"
AIRFLOW_REDSHIFT_ID = "redshift"


default_args = {
    'owner': 'Sparkify',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 13),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}


@dag(default_args=default_args,
     schedule_interval='@hourly')
def etl():
    # airflow variables
    s3_bucket = Variable.get('s3_bucket')
    log_data_suffix = Variable.get('s3_prefix_log_data')
    song_data_suffix = Variable.get('s3_prefix_song_data')
    log_json_path_suffix = Variable.get('s3_prefix_log_json_path')
    region = Variable.get('region')

    begin_execution = EmptyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        # connection id for Redshift configured in Airflow
        redshift_conn_id=AIRFLOW_REDSHIFT_ID,
        # connection id for IAM configured in Airflow
        aws_credentials_id=AIRFLOW_AWS_CONN_ID,
        table='staging_events',
        s3_bucket=s3_bucket,
        s3_key=log_data_suffix,
        region=region,
        file_format='JSON',
        s3_json_paths_format=log_json_path_suffix)

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket=s3_bucket,
        s3_key=song_data_suffix,
        region=region,
        file_format='JSON')

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql_query=SqlQueries.songplay_table_insert
    )

    load_songs_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql_query=SqlQueries.song_table_insert
    )

    load_users_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql_query=SqlQueries.user_table_insert
    )

    load_artist_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql_query=SqlQueries.artist_table_insert
    )

    load_time_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql_query=SqlQueries.time_table_insert
    )

    data_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        data_quality_count_checks=SqlQueries.all_data_quality_count_checks,
        data_quality_null_checks=SqlQueries.all_data_quality_null_checks
    )

    end_execution = EmptyOperator(task_id='End_execution')

    begin_execution >> [stage_events_to_redshift, stage_songs_to_redshift] >> \
    load_songplays_table >> \
    [load_songs_table, load_users_table, load_artist_table, load_time_table] >> \
    data_quality_checks >> end_execution


etl_dag = etl()
