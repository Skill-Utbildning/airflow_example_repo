import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago

# Import your custom functions for reading file and writing to Oracle database
from scripts.read_file_contents import read_file_contents
from scripts.write_to_oracle import write_to_oracle

CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    }

# Create the DAG instance
with DAG(
    'file_to_oracle_dag',
    default_args=default_args,
    schedule_interval=None, # You can set the schedule_interval as needed
    catchup=False, # Catchup to avoid backfilling, set as needed
 ) as dag:
  # Define the FileSensor task
  file_sensor_task = FileSensor(
    task_id='file_sensor_task',
    poke_interval=300,
    filepath= os.path.join(CURR_DIR_PATH, 'ingest_source','*.csv'), # Check for the file every 5 minutes
  )
  # Define the PythonOperator task to read the file
  read_file_task = PythonOperator(
    task_id='read_file_task',
    python_callable=read_file_contents, # Your custom Python function
    op_kwargs={'file_path' : os.path.join(CURR_DIR_PATH, 'ingest_source','*.csv')}
  )
  # Define the PythonOperator task to write to Oracle database
  write_to_oracle_task = PythonOperator(
    task_id='write_to_oracle_task',
    python_callable=write_to_oracle, # Your custom Python function
    op_args=['{{ task_instance(task_ids="read_file_task") }}'], # Pass the file contents
  )
  # Define the task dependencies
  file_sensor_task >> read_file_task >> write_to_oracle_task