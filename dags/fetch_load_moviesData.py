from airflow.sdk import dag,task
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.email import send_email


def notify_failure(context):
    ti=context['ti']

    subject = f"❌ Task Failed: {ti.task_id}"
    body = f"""
    DAG: {ti.dag_id} <br>
    Task: {ti.task_id} <br>
    Execution Date: {context.get('logical_date')} <br>
    Log URL: {ti.log_url} <br>
    """
    send_email(
        to='pratikchandel99@gmail.com',
        subject=subject,
        html_content=body
    )

@dag(
        start_date=datetime(2026,1,1),
        schedule=None,
        catchup=None,
        on_failure_callback=notify_failure
)
def load_raw_full_movies():

    @task(on_failure_callback=notify_failure)
    def create_table():
        hook=SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql="""create or replace table netflix_raw_database.raw.movies(
                movieId int,
                title varchar,
                genres varchar
                );"""
        hook.run(sql)

    @task(on_failure_callback=notify_failure)
    def load_s3_to_snowflake():
        hook=SnowflakeHook(snowflake_conn_id='snowflake_conn')
        sql="""
            USE WAREHOUSE TRANSFORMING;
            USE DATABASE netflix_raw_database;
            USE SCHEMA raw;     

            copy into netflix_raw_database.raw.movies
            from @netflix_raw_database.raw.netflixstage/movies.csv
            FILE_FORMAT=(
            TYPE =CSV 
            FIELD_DELIMITER=','
            SKIP_HEADER=1
            )
            ON_ERROR='CONTINUE';"""
        hook.run(sql) 
        count=hook.get_records("select count(*) from netflix_raw_database.raw.movies;")[0][0]
        return count
    
    @task
    def send_consolidated_mail(count):
        subject = f"✅ Task Successfully Completed "
        body = f"""
                All tasks completed successfully.<br>
                Table: Movies<br>
                Rows Loaded: {count} 
                """
        send_email(
            to='pratikchandel99@gmail.com',
            subject=subject,
            html_content=body
            
        ) 

    create=create_table()
    load=load_s3_to_snowflake()
    mail=send_consolidated_mail(load)
    create >> load >> mail
load_raw_full_movies()    