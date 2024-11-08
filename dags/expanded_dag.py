# Slightly expanded upon my_dag.py YouTube code-along

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

from random import randint
from datetime import datetime
import os
import configparser

### Finding target folder ###
## Option 1 ##
# Will resolve to the folder where this file exists
CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))


# TARGET_FOLDER = AIRFLOW_DAGS
TARGET_FOLDER = CURR_DIR_PATH


def _choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])

    best_accuracy = max(accuracies)

    if (best_accuracy > 8):
        return 'accurate'
    else:
        return 'inaccurate'


def _training_model():
    return randint(1, 10)


# Version 1.x+ syntax
with DAG(
        "my_dag",
        start_date=datetime(2021, 1, 1),
        # A manual trigger of the DAG won't run thoroughly if today's date is earlier than start_date
        schedule="0 * * * *",  # Hourly with cron syntax, equivalent to "@hourly"
        catchup=False
):
    training_model_A = PythonOperator(
        task_id="training_model_A",
        python_callable=_training_model
    )

    training_model_B = PythonOperator(
        task_id="training_model_B",
        python_callable=_training_model
    )

    training_model_C = PythonOperator(
        task_id="training_model_C",
        python_callable=_training_model
    )

    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=_choose_best_model
    )

    # Note the bash_command, it does the following:
    # echo - (print out whatever follows)
    # accurate $dt_now - "accurate" the string and $dt_now will fetch the value for the 'dt_now' environment variable if it exists
    # >> - redirect the output from the terminal itself
    # >> results.txt - redirect the output from the terminal to a file called 'results.txt' (relative path, using the current working directory)
    #
    # The extra 'dt_now' environment variable is provided with the 'env' parameter for the BashOperator. Takes a Python dict with key:value => env_variable:value.
    accurate = BashOperator(
        task_id="accurate",
        bash_command=f"echo \"accurate '$dt_now'\" >> results.txt",
        env={"dt_now": str(datetime.now())},
        cwd=TARGET_FOLDER
        # This is the folder we determined further up. 'cwd' parameter changes the working directory for the script from /tmp (default for Airflow tasks in Linux) to the path value we provide here
    )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command=f"echo \"inaccurate '$dt_now'\" >> results.txt",
        env={"dt_now": str(datetime.now())},
        cwd=TARGET_FOLDER
    )

    [training_model_A, training_model_B, training_model_C] >> choose_best_model >> [accurate, inaccurate]