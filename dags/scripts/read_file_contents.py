import os
import json
import pandas as pd

def read_file_contents(file_path):

    ingested_df = pd.read_csv(file_path)

    CURR_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
    bronze_target = os.path.join(CURR_DIR_PATH, '..', 'data', 'bronze', 'raw.json')

    ingested_df.to_json(bronze_target)

    return bronze_target