from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          # start_date=datetime.now()
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Added create tables task
# create_tables_task = PostgresOperator(
#     task_id="create_tables",
#     dag=dag,
#     postgres_conn_id="redshift",
#     sql='create_tables.sql'
# )

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_path='s3://udacity-dend/log_data/',
    json_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_path='s3://udacity-dend/song_data/A/A/A/',
    json_path='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',  
    sql = SqlQueries.songplay_table_insert,
    delete_records = True
)

# Changed users to users1 due to a users table already existing as part of sample database in the redshift cluster
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users1',  
    sql = SqlQueries.user_table_insert,
    delete_records = True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',  
    sql = SqlQueries.song_table_insert,
    delete_records = True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',  
    sql = SqlQueries.artist_table_insert,
    delete_records = True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',  
    sql = SqlQueries.time_table_insert,
    delete_records = True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tables=['songplays', 'users1', 'songs', 'artists', 'time'],
    dq_checks=[
        {'check_sql': 'SELECT COUNT(*) FROM artists WHERE artistid IS NULL',
         'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM songplays WHERE playid IS NULL',
         'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM songs WHERE songid IS NULL',
         'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM time WHERE start_time IS NULL',
         'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM users1 WHERE userid IS NULL',
         'expected_result': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# start_operator >> create_tables_task

# create_tables_task >> stage_events_to_redshift
# create_tables_task >> stage_songs_to_redshift

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
