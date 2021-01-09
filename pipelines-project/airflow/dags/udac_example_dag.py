import os
from airflow import DAG
from helpers import SqlQueries
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from constants import (S3_BUCKET_NAME, REDSHIFT_CONN_ID, APPEND_MODE,
                       DEFAULT_ARGS, AWS_CONN_ID)

dag = DAG('udac_example_dag',
          default_args=DEFAULT_ARGS,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    redshift_conn_id=REDSHIFT_CONN_ID,
    s3_bucket=S3_BUCKET_NAME,
    s3_path="log_data",
    task_id='Stage_events',
    table_name="staging_events",
    json_paths="s3://udacity-dend/log_json_path.json",
    aws_conn_id=AWS_CONN_ID,
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    redshift_conn_id=REDSHIFT_CONN_ID,
    s3_bucket=S3_BUCKET_NAME,
    s3_path="song_data",
    table_name="staging_songs",
    aws_conn_id=AWS_CONN_ID,
    task_id='Stage_songs',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table_name="songplays",
    data_qry=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table_name="users",
    data_qry=SqlQueries.user_table_insert,
    append_data=APPEND_MODE,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table_name="songs",
    append_data=APPEND_MODE,
    data_qry=SqlQueries.song_table_insert,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table_name="artists",
    append_data=APPEND_MODE,
    data_qry=SqlQueries.artist_table_insert,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id=REDSHIFT_CONN_ID,
    table_name="time",
    append_data=APPEND_MODE,
    data_qry=SqlQueries.time_table_insert,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id=REDSHIFT_CONN_ID,
    check_tables=["songplays", "users", "songs", "time", "artists"],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Set dependencies
start_operator \
    >> [stage_songs_to_redshift, stage_events_to_redshift] \
    >> load_songplays_table \
    >> [
        load_time_dimension_table,
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table
    ] \
    >> run_quality_checks \
    >> end_operator

