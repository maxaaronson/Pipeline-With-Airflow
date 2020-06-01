import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import (DataQualityOperator, LoadDimensionOperator,
                               LoadFactOperator, StageToRedshiftOperator)
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries

# arguments passed to the dag
default_args = {
    'owner': 'Max',
    'start_date': datetime(2019, 9, 23),
    'Depends_on_past': True,
    'Retries': 1,
    'Retry_delay': timedelta(minutes=1),
    'Catchup': ''
}

# create a dag object to be used by Airflow
dag = DAG(
    'project_5',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)

# dummy start operator
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# copy S3 data to the staging_events table
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    S3_path='s3://udacity-dend/log_data/2018/11',
    staging_table='staging_events',
    redshift_conn_id='redshift',
    file_type='JSON',
    json_path='s3://udacity-dend/log_json_path.json',
    ARN='arn:aws:iam::631154594748:role/myRedshiftRole',
    dag=dag
)

# copy S3 data to the staging_songs table
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    S3_path='s3://udacity-dend/song_data/A/B/C/',
    staging_table='staging_songs',
    redshift_conn_id='redshift',
    file_type='JSON',
    # ARN=  [ ARN with S3 read access ]
    dag=dag
)

# bulk insert data into songplays
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    sql=SqlQueries.songplay_table_insert,
    table_name='songplays',
    redshift_conn_id='redshift',
    dag=dag
)

# bulk insert into the songs, artists,
# users and time tables
# (truncate option can be specified)
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    sql=SqlQueries.user_table_insert,
    truncate_table=True,
    table_name='users',
    redshift_conn_id='redshift',
    dag=dag)
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    sql=SqlQueries.song_table_insert,
    truncate_table=True,
    table_name='songs',
    redshift_conn_id='redshift',
    dag=dag)
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    sql=SqlQueries.artist_table_insert,
    truncate_table=True,
    table_name='artists',
    redshift_conn_id='redshift',
    dag=dag)
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    sql=SqlQueries.time_table_insert,
    truncate_table=True,
    table_name='time',
    redshift_conn_id='redshift',
    dag=dag)

# run quality checks on a list of tables
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    table_list=[
        'songs',
        'artists',
        'users',
        'time',
        'songplays'],
    retries=5,
    redshift_conn_id='redshift',
    dag=dag
)

# dummy end operator
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# set dependency flow for tasks
start_operator >> [
    stage_events_to_redshift, stage_songs_to_redshift
] >> load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
] >> run_quality_checks >> end_operator
