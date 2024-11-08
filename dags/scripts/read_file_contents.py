import os
import pandas as pd



def read_file_contents(file_path):

    ingested_df = pd.read_csv('/opt/airflow/dags/ingest_source/demodata.csv')
    CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
    bronze_target = os.path.join(CURR_DIR_PATH, 'data', 'bronze')
    if not os.path.exists(bronze_target):
        os.mkdir(bronze_target)

    ingested_df.to_json(bronze_target+'/raw.json')

    return bronze_target