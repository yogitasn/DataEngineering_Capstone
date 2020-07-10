from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook

from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)
from helpers import SqlQueries
# Define default arguments


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly'
        )

# Define dag variables
project_id = 'cloud-data-lake'
staging_dataset = 'IMMIGRATION_DWH_STAGING'
dwh_dataset = 'IMMIGRATION_DWH'
s3_bucket = 'cloud-data-lake-gcp'

start_pipeline = DummyOperator(task_id='Begin_execution',  dag=dag)

emrsshHook= SSHHook(ssh_conn_id='emr_ssh_connection')

jobOperator = SSHOperator(
    task_id="ImmigrationETLJob",
    command='cd /home/hadoop/immigration_etl_pipeline/src;export PYSPARK_DRIVER_PYTHON=python3;export PYSPARK_PYTHON=python3;spark-submit --master yarn immigration_driver.py;',
    ssh_hook=emrsshHook,
    dag=dag)

stage_immigration_to_redshift = StageToRedshiftOperator(
    task_id="Stage_immigration",
    dag=dag,
    table="staging_immigration",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacitycapstoneproject",
    s3_key='sasdata',
    json_path='*//.parquet',
    create_stmt=SqlQueries.create_table_staging_immigration
)

stage_us_cities_demographics_to_redshift = StageToRedshiftOperator(
    task_id="Stage_us_citeventsies_demographics",
    dag=dag,
    table="staging_us_cities_demographics",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacitycapstoneproject",
    s3_key='demo',
    csv_path='us-cities-demographics.csv',
    create_stmt=SqlQueries.create_table_staging_us_cities_demographics
)

stage_airport_to_redshift = StageToRedshiftOperator(
    task_id="Stage_airport",
    dag=dag,
    table="staging_airport",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacitycapstoneproject",
    s3_key="airport/",
    json_path="airport-codes_csv.csv",
    create_stmt=SqlQueries.create_table_staging_airport
    
)

loaded_data_to_staging = DummyOperator(
    task_id = 'loaded_data_to_staging'
)


load_country = StageToRedshiftOperator(
    task_id="load_country",
    dag=dag,
    table="d_country",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacitycapstoneproject",
    s3_key="rescitycntry/",
    json_path="I94City_Res.csv",
    create_stmt=SqlQueries.create_table_d_country
)

load_state = StageToRedshiftOperator(
    task_id="load_state",
    dag=dag,
    table="d_state",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacitycapstoneproject",
    s3_key="addrstate/",
    json_path="I94ADDR_State.csv",
    create_stmt=SqlQueries.create_table_d_state
    
)
load_port = StageToRedshiftOperator(
    task_id="load_port",
    dag=dag,
    table="d_port",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacitycapstoneproject",
    s3_key="port/",
    json_path="/I94_Port.csv",
    create_stmt=SqlQueries.create_table_d_port
    
)

# Check loaded data not null


run_quality_checks_airports = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
    ]
)

run_quality_checks_us_cities_demo = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
    ]

)

run_quality_checks_immigration = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
    ]

)


create_final_immigration_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    table="final_immigration",
    create_stmt=SqlQueries.create_table_final_immigration,
    insert_stmt=SqlQueries.final_immigration_table_insert
)

run_quality_checks_final_immigration_data = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    redshift_conn_id="redshift",
    dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
    ]

)

create_D_CITY_DEMO = LoadDimensionOperator(
    task_id="create_D_CITY_DEMO",
    dag=dag,
    redshift_conn_id="redshift",
    append_data="",
    table="D_CITY_DEMO",
    stmt=SqlQueries.D_CITY_DEMO_INSERT

)


create_d_airport = LoadDimensionOperator(
    task_id="create_d_airport",
    dag=dag,
    redshift_conn_id="redshift",
    append_data="",
    table="D_AIRPORT",
    stmt=SqlQueries.d_airport_insert
)

create_d_time = LoadDimensionOperator(
    task_id="create_d_time",
    dag=dag,
    redshift_conn_id="redshift",
    append_data="",
    table="D_TIME",
    stmt=SqlQueries.D_TIME_INSERT
)




finish_pipeline = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define task dependencies
dag >> start_pipeline >> jobOperator >> [stage_immigration_to_redshift, stage_us_cities_demographics_to_redshift, stage_airport_to_redshift]

stage_immigration_to_redshift >> run_quality_checks_immigration
stage_us_cities_demographics_to_redshift >> run_quality_checks_us_cities_demo
stage_airport_to_redshift >> run_quality_checks_airports


[run_quality_checks_immigration, run_quality_checks_us_cities_demo, run_quality_checks_airports] >> loaded_data_to_staging

loaded_data_to_staging >> [load_country, load_port, load_state] >> create_final_immigration_table >> run_quality_checks_final_immigration_data

run_quality_checks_final_immigration_data >> [create_d_time, create_d_airport, create_d_city_demo] >> finish_pipeline
