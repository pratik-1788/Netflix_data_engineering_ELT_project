from airflow.sdk import dag,task
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.email import send_email

@dag
def load_raw_full_tag():
    try:

        @task.python
        def create_table():
            hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
            sql="""create or replace table netflix_database.staging.tags(userId int,movieId int,tag varchar,timestamp varchar);"""
            hook.run(sql)

        @task.python
        def load_s3_to_snowflake():
            try:
                
                hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
                sql="""
                        
                        USE WAREHOUSE TRANSFORMING;
                        USE DATABASE netflix_database;
                        USE SCHEMA staging;
                        copy into netflix_database.staging.tags
                        from @netflix_database.staging.netflixstage/tags
                        FILE_FORMAT = (
                        TYPE = CSV 
                        FIELD_DELIMITER = ','
                        SKIP_HEADER = 1
                        )
                        ON_ERROR = 'CONTINUE';
                    """
                hook.run(sql)
                count=hook.get_records("select count(*) from netflix_database.staging.tags ")
                return count
            except Exception as e:
                print(e)      
        @task.python
        def send_consolidated_mail(count):
            
            if count==0:
                subject = f"❌ Task Failed: "
                body =  f"""
                Task failed or no data loaded.<br>
                Rows Loaded: {count}
                """
            else:
                subject = f" Task successfully completed:  and loaded rows {count}"
                body =  f"""
                Task completed successfully.<br>
                Rows Loaded: {count}
                """
            send_email(
                    to="pratikchandel788@gmail.com",
                    subject=subject,
                    html_content=body
                )    
    except Exception as e:
        print(e)    

    create_table=create_table() 
    load_data=load_s3_to_snowflake()
    send_mail=send_consolidated_mail(load_data)

    create_table >> send_mail
load_raw_full_tag()