from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# Define fetch and load data function
def fetch_load_data():

    response = requests.get("https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=fnd&lang=tc")
    forecast = response.json()

    data = []
    for w in forecast['weatherForecast']:
        try:
            date = datetime.strptime( f"{w['forecastDate'][:4]}-{w['forecastDate'][4:6]}-{w['forecastDate'][6:]}", '%Y-%m-%d' ).date() 
            w.update( {'forecastDate' : date } )
            for k in ('forecastMaxtemp','forecastMintemp','forecastMaxrh','forecastMinrh'):
                w.update( {'table_name' : f"{w['forecastDate'][:4]}_{w['forecastDate'][4:6]}_{w['forecastDate'][6:]}" } )
                w.update( { k : ''.join( [ str(v).replace('percent','%') for v in w[k].values() ] ) } )
            data.append(w)
        except Exception as e:
            print( f'UNABLE TO TRANSFORM w as {e}, SKIPPING... w = {w}' )

    partition_table_query = '\n'.join( [ f"CREATE TABLE IF NOT EXIST {d['table_name']} PARTITION OF forecast FOR VALUES ('{d['forecastDate']}');" for d in data ] )

    conn = psycopg2.connect(
            database="postgres",
            user="postgres",
            password="5432",
            host="localhost",
            port='5432',)
    cur = conn.cursor()
    cur.execute(f"""CREATE TABLE forecast IF NOT EXIST (
                        forecastDate   DATE NOT NULL,
                        week        VARCHAR,
                        forecastWind       VARCHAR,
                        forecastWeather       VARCHAR,
                        forecastMaxtemp       VARCHAR,
                        forecastMintemp       VARCHAR,
                        forecastMaxrh       VARCHAR,
                        forecastMinrh       VARCHAR,
                        ForecastIcon       INTERGER,
                        PSR       VARCHAR,)
                    PARTITION BY RANGE ( forecastDate );
                    {partition_table_query}""")
    columns = [ k for k in data[0].keys() if k != 'table_name' ]
    query = "INSERT INTO forecast ({}) VALUES %s".format(','.join(columns))
    values = [ [value for k, value in d.items() if k != 'table_name' ] for d in data ]
    execute_values(cur, query, values)
    print ("Data Inserted")
    conn.commit()
    cur.close()
    conn.close()


# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 29),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Define the DAG
dag = DAG(
    'hko_weather_forecast',
    default_args=default_args,
    description='Fetch HKO weather forecast and load into PostgreSQL',
    schedule_interval='0 8 * * *',  #   once a day at 8:00 am HKT
    catchup=False,
)


# Define the fetch data task
fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_load_data,
    dag=dag,
)


# Set the task dependencies
fetch_data_task