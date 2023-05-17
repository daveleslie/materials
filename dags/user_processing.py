from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
from datetime import datetime
from pandas import json_normalize

# Transform function. I guess in theory this could be a python script to transform raw meter data into daily records
def _process_user(ti):
	user = ti.xcom_pull(task_ids='extract_user')
	user = user['results'][0]
	processed_user = json_normalize({
		'firstname': user['name']['first'],
		'lastname': user['name']['last'],
		'country': user['location']['country'],
		'username': user['login']['username'],
		'password': user['login']['password'],
		'email':  user['email']
	})
	processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)

# Load function
def _store_user():
	hook = PostgresHook(postgres_conn_id='postgres')
	hook.copy_expert(
		sql="COPY users FROM STDIN WITH DELIMITER AS ','",
		filename='/tmp/processed_user.csv'
	)

with DAG('user_processing', start_date=datetime(2022, 1, 1),
        schedule_interval='@daily', catchup=False) as dag: 

	# I assume this is where the data will be loaded at the end of the DAG run
	create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres',
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        '''
    )
    
	# Check if API is available. End task run here if not available?
	is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/' 
	)

	# Extract task
	extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='api/',
		method='GET',
		response_filter=lambda response: json.loads(response.text),
		log_response=True
	)

	# Transform task
	process_user = PythonOperator(
		task_id='process_user',
		python_callable=_process_user
	)

	# Load task
	store_user = PythonOperator(
		task_id='store_user',
		python_callable=_store_user
	)

	# Define task dependencies. i.e. 'process_user' is dependent on 'extract_user', etc.
	create_table >> is_api_available >> extract_user >> process_user >> store_user
