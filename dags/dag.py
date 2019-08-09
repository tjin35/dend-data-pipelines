from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, 
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from subdag import load_dimension_dag

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

start_date = datetime(2019, 1, 12)
default_args = {
    'owner': 'udacity',
    'start_date': start_date,
    'Depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          catchup = False,
          schedule_interval='0 * * * *'
        )

start_operator= DummyOperator(task_id='Begin_execution',  dag=dag)

create_staging_event_table = PostgresOperator(
        task_id='Create_staging_events_table',
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.create_staging_events_table
)

create_staging_song_table = PostgresOperator(
        task_id='Create_staging_songs_table',
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.create_staging_songs_table
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="public.staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json="s3://udacity-dend/log_json_path.json" 
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="public.staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json="auto" 
)

create_staging_events_table = PostgresOperator(
        task_id='Create_staging_events_table',
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.create_staging_events_table
)

create_songplays_table = PostgresOperator(
        task_id='Create_songplays_table',
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.create_songplays_table
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.songplays_table_insert
)

user_subdag_task = SubDagOperator(
    subdag=load_dimension_dag(
        "dag",
        "user_subdag",
        "redshift",
        "users",
        SqlQueries.create_users_table,
        SqlQueries.users_table_insert,
        start_date=start_date
    ),
    task_id="user_subdag",
    dag=dag,  
)

song_subdag_task = SubDagOperator(
    subdag=load_dimension_dag(
        "dag",
        "song_subdag",
        "redshift",
        "songs",
        SqlQueries.create_songs_table,
        SqlQueries.songs_table_insert,
        start_date=start_date
    ),
    task_id="song_subdag",
    dag=dag,
)

artist_subdag_task = SubDagOperator(
    subdag=load_dimension_dag(
        "dag",
        "artist_subdag",
        "redshift",
        "artists",
        SqlQueries.create_artists_table,
        SqlQueries.artists_table_insert,
        start_date=start_date
    ),
    task_id="artist_subdag",
    dag=dag,
)

time_subdag_task = SubDagOperator(
    subdag=load_dimension_dag(
        "dag",
        "time_subdag",
        "redshift",
        "time",
        SqlQueries.create_time_table,
        SqlQueries.time_table_insert,
        start_date=start_date
    ),
    task_id="time_subdag",
    dag=dag,
)

table_list = ["songplays","time","artists","songs","users"]
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=table_list
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> (create_staging_song_table,create_staging_event_table) 
create_staging_song_table >> stage_songs_to_redshift >> create_songplays_table
create_staging_event_table >> stage_events_to_redshift >> create_songplays_table
create_songplays_table >> load_songplays_table >> (user_subdag_task,song_subdag_task,artist_subdag_task,time_subdag_task) >> run_quality_checks >> end_operator