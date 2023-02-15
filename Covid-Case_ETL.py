import requests
import pandas as pd
import boto3
from tqdm import tqdm
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def extract_data():
    # Send a GET request to the API
    headers = {
        "X-RapidAPI-Key": "**********************",
        "X-RapidAPI-Host": "**********************"
    }
    response = requests.request("GET", "https://vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com/api/npm-covid-data/", headers=headers)
    json_data = response.json()
    
    return json_data

def transform_data(json_data):

    covid_data = pd.DataFrame.from_dict(json_data)

    # Creating the cv19_country table
    cv19_country = covid_data[["Country", "Continent", "TwoLetterSymbol", "ThreeLetterSymbol"]].rename(
        columns={
            "Country": "country_name",
            "Continent": "continent",
            "TwoLetterSymbol": "country_code",
            "ThreeLetterSymbol": "iso3"
        }
    )
    # Add an id column as the primary key
    cv19_country.insert(0, "country_id", range(1, len(cv19_country)+1))

    # Creating the cv19_metrics table
    cv19_metrics= covid_data[["rank", "Infection_Risk", "Case_Fatality_Rate", "Test_Percentage", "Recovery_Proporation", "Deaths_1M_pop","Serious_Critical"]].rename(
        columns={
            "rank": "country_rank",
            "Infection_Risk": "infection_risk",
            "Case_Fatality_Rate": "case_rates",
            "Test_Percentage": "test_percent",
            "Recovery_Proporation": "recovery_props",
            "Deaths_1M_pop": "deaths_1M_pop",
            "Serious_Critical": "critical_numbers"
        }
    )
    # Add an id column as the primary key
    cv19_metrics.insert(0, "metrics_id", range(1, len(cv19_metrics)+1))

    # Creating the cv19_case table
    cv19_case = covid_data[["TotalCases", "NewCases", "TotalRecovered", "ActiveCases", "TotalTests"]].rename(
        columns={
            "TotalCases": "total_cases",
            "NewCases": "new_cases",
            "TotalRecovered": "recovered",
            "ActiveCases": "active_cases",
            "TotalTests": "total_test"
        }
    )
    # Add an id column as the primary key
    cv19_case.insert(0, "case_id", range(1, len(cv19_case)+1))

    # Creating the  cv19_population table
    cv19_population = covid_data[["Population", "one_Caseevery_X_ppl", "one_Deathevery_X_ppl", "one_Testevery_X_ppl", "Tests_1M_Pop", "TotCases_1M_Pop"]].rename(
        columns={
            "Population": "population",
            "one_Caseevery_X_ppl": "one_case_every_X_ppl",
            "one_Deathevery_X_ppl": "one_death_every_X_ppl",
            "one_Testevery_X_ppl": "one_Testevery_X_ppl",
            "Tests_1M_Pop": "Test_1M_pop",
            "TotCases_1M_Pop": "TotCases_1M_pop"
        }
    )
    
    # Add an id column as the primary key
    cv19_population.insert(0, "population_id", range(1, len(cv19_population)+1))

    return cv19_country, cv19_metrics, cv19_case, cv19_population,json_data

def load_data(cv19_country, cv19_metrics, cv19_case, cv19_population,json_data):
    # Creating a session to access Amazon S3
    session = boto3.Session(
        aws_access_key_id="**********************",
        aws_secret_access_key="**********************"
    )
    s3 = session.client("s3")

    # Writing the cv19_country data to Amazon S3
    s3.put_object(Bucket='projectsaya', Key='covid_case_project/cv19_country.parquet', Body=cv19_country.to_parquet(index=False))

    # Writing the cv19_metrics data to Amazon S3
    s3.put_object(Bucket='projectsaya', Key='covid_case_project/cv19_metrics.parquet', Body=cv19_metrics.to_parquet(index=False))

    # Writing the cv19_case data to Amazon S3
    s3.put_object(Bucket='projectsaya', Key='covid_case_project/cv19_case.parquet', Body=cv19_case.to_parquet(index=False))

    # Writing the cv19_population data to Amazon S3
    s3.put_object(Bucket='projectsaya', Key='covid_case_project/cv19_population.parquet', Body=cv19_population.to_parquet(index=False))

    # Writing the raw_API_data to Amazon S3
    s3.put_object(Bucket='projectsaya', Key='covid_case_project/raw_API_data.json', Body=json.dumps(json_data).encode('utf-8'))


#ETL Process
print("start extract process::")
try:
    with tqdm(total=100) as pbar:
        covid_data = extract_data()
        pbar.update(100)
    print("extract finished::")
except:
    print("extract error::")

print("tranform stage starting::")
try:
    with tqdm(total=100) as pbar:
        cv19_country, cv19_metrics, cv19_case, cv19_population, json_data = transform_data(covid_data)
        pbar.update(100)
    print("transform finished::")
except:
    print("transform error")
print('loading stage started::')
try:
    with tqdm(total=100) as pbar:
        load_data(cv19_country, cv19_metrics, cv19_case, cv19_population,json_data)
        pbar.update(100)
    print('load completed')
except:
    print('load error')


dag = DAG(
    'covid_tracker_etl_2',
    description='Extract, Transform and Load Covid Tracker data using Apache Airflow',
    schedule_interval='10 0 * * *',
    start_date=datetime(2023, 2, 12),
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_covid_tracker_data',
    python_callable = extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_covid_data',
    python_callable = transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data_intoS3',
    python_callable = load_data,
    dag=dag
)

extract_task >> transform_task >> load_task
