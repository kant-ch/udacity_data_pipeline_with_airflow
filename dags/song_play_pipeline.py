from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    "owner": "Kant Charoensedtasin",
    "start_date": pendulum.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def song_play_pipeline():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        iam_role="arn:aws:iam::963000201285:role/myRedshiftRole",
        table="staging_events",
        s3_bucket="udacity-final-project20231208",
        s3_key="log-data",
        json_path="log_json_path.json"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        iam_role="arn:aws:iam::963000201285:role/myRedshiftRole",
        table="staging_songs",
        s3_bucket="udacity-final-project20231208",
        s3_key="song-data",
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table='songsplay',
        insert_sql=SqlQueries.songplay_table_insert,
        delete_load=False,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table='users',
        insert_sql=SqlQueries.user_table_insert,
        delete_load=False,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table='songs',
        insert_sql=SqlQueries.song_table_insert,
        delete_load=False,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table='artists',
        insert_sql=SqlQueries.artist_table_insert,
        delete_load=False,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table='time',
        insert_sql=SqlQueries.time_table_insert,
        delete_load=False,
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        check=[
            {
                'test_case': 'Check null in play_id column of songplays table',
                'check_sql': 'select count(*) from songplays where playid is null',
                'expected_result': 0,
                'compare_type': '='
            },
            {
                'test_case': 'Check record of table user greater than 0',
                'check_sql': 'select count(*) from users',
                'expected_result': 0,
                'compare_type': '>'
            },
            {
                'test_case': 'Check year of the songs released from songs table',
                'check_sql': "select count(*) from songs where year > extract(year from CURRENT_DATE)",
                'expected_result': 1,
                'compare_type': '<'
            },
        ]
    )

    end_operator = DummyOperator(task_id='End_execution')
    
    # Task dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,load_time_dimension_table]
    [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

song_play_pipeline_dag = song_play_pipeline()
