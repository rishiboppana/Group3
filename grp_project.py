from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
import requests
import pandas as pd
import os 
from datetime import timedelta
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


def connection() :
    hook = SnowflakeHook(snowflake_conn_id = 'snowflake_academia' )
    con =  hook.get_conn()

    return con.cursor()

@task
def initialising():
    con = connection()
    con.execute("Create database if not exists nasa")
    con.execute("use database nasa")
    schema ="raw"
    con.execute(f"""Create schema if not exists {schema}""")
    con.execute("use schema raw ")


    table = "nasa_neo_table"

    con.execute(f"""CREATE TABLE if not exists {table} (
        Close_Approach_Date DATE,
        Name varchar,
        ID int,
        Estimated_Diameter_Min_km decimal(21,18),
        Estimated_Diameter_Max_km decimal(21,18),
        Absolute_Magnitude_H decimal(7,4),
        Miss_Distance_km decimal(22,11),
        Miss_Distance_Lunar decimal(20,16),
        Miss_Distance_AU decimal(17,13),
        Relative_Velocity_kmh decimal(18,12),
        Orbiting_Body varchar(10),
        Is_Sentry_Object varchar(6)  
    )""")

@task
def extract():

    API_KEY = Variable.get("Nasa_Api")

    # Defining Date range: last 365 days
    end_date = datetime.today()
    start_date = end_date - timedelta(days=365)

    # Creating empty list to store Asteroid data
    all_asteroids = []

    # Looping through each 7-day interval
    current_date = start_date
    while current_date < end_date:
        next_date = current_date + timedelta(days=7)
        if next_date > end_date:
            next_date = end_date

        # Formating dates for API request
        start_str = current_date.strftime("%Y-%m-%d")
        end_str = next_date.strftime("%Y-%m-%d")

        # NASA API request
        url = f"https://api.nasa.gov/neo/rest/v1/feed?start_date={start_str}&end_date={end_str}&api_key={API_KEY}"
        response = requests.get(url)
        data = response.json()

        # Extracting Asteroid data
        neo_data = data.get("near_earth_objects", {})
        for date, asteroids in neo_data.items():
            for asteroid in asteroids:
                asteroid_info = {
                    "Close_Approach_Date": asteroid["close_approach_data"][0]["close_approach_date"],
                    "Name": asteroid["name"],
                    "ID": asteroid["id"],
                    "Estimated_Diameter_Min_km": asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_min"],
                    "Estimated_Diameter_Max_km": asteroid["estimated_diameter"]["kilometers"]["estimated_diameter_max"],
                    "Absolute_Magnitude_H": asteroid["absolute_magnitude_h"],
                    "Miss_Distance_km": asteroid["close_approach_data"][0]["miss_distance"]["kilometers"],
                    "Miss_Distance_Lunar": asteroid["close_approach_data"][0]["miss_distance"]["lunar"],
                    "Miss_Distance_AU": asteroid["close_approach_data"][0]["miss_distance"]["astronomical"],
                    "Relative_Velocity_kmh": asteroid["close_approach_data"][0]["relative_velocity"]["kilometers_per_hour"],
                    "Orbiting_Body": asteroid["close_approach_data"][0]["orbiting_body"],
                    "Is_Sentry_Object": asteroid["is_sentry_object"]
                }
                all_asteroids.append(asteroid_info)

        # Moving to the next 7-day window
        current_date = next_date + timedelta(days=1)

    # Converting collected data into a DataFrame
    df = pd.DataFrame(all_asteroids)
    return df 

@task
def transfer(df):
    if os.path.exists("Nasa_data.tmp"):
        os.remove("Nasa_Data.tmp")

    df.to_csv("Nasa_Data.tmp",index = False)
    return os.path.abspath("Nasa_Data.tmp")

@task
def load(file_path):
    con = connection()
    con.execute("use database nasa")
    con.execute("use schema raw")
    try :
        con.execute("begin")
        con.execute("create or replace stage nasa_stage")
        con.execute("truncate table nasa_neo_table")
        con.execute(f"PUT file://{file_path} @nasa_stage AUTO_COMPRESS=FALSE")
        con.execute("""COPY INTO nasa_neo_table
                        FROM @nasa_stage/Nasa_Data.tmp
                        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
                    """)
    except Exception as e :
        print(e)
        con.execute("rollback")
    finally:
        con.execute("commit")
with DAG(
    dag_id='Nasa_Dag',  
    start_date=datetime(2024, 3, 1),  
    catchup=False,  
    tags=['Nasa_ETL'],  
    schedule_interval='0 8 * * *'  
) as dag:
    def connection() :
        hook = SnowflakeHook(snowflake_conn_id = 'snowflake_academia' )
        con =  hook.get_conn()

        return con.cursor()
    con = connection()
    e = extract()
    t = transfer(e)
    l = load(t)
    trigger_dag = TriggerDagRunOperator(
        task_id = 'trigger_dag',
        trigger_dag_id= 'BuildELT_dbt',
        wait_for_completion=True,
        retries = 1
    )

    e>>t>>l>>trigger_dag