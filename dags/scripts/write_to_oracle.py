import oracledb
import json
from datetime import datetime

def write_to_oracle(ti):

    file_path=ti.xcom_pull(task_ids='read_file_task')

    # Oracle connection details
    oracle_username = 'your_username'
    oracle_password = 'your_password'
    oracle_host = 'your_host'
    oracle_port = 'your_port'
    oracle_service_name = 'your_service_name'

    # Read the JSON file
    with open(file_path, 'r') as json_file:
        records = json.load(json_file)

    # Connect to the Oracle database
    oracle_dsn = oracledb.makedsn(oracle_host, oracle_port, service_name=oracle_service_name)
    connection = oracledb.connect(user=oracle_username, password=oracle_password, dsn=oracle_dsn)

    try:
        cursor = connection.cursor()

        # Insert records into the Oracle table
        for record in records:
            as_of_date = datetime.now().date()
            sql = """
                INSERT INTO your_table_name (COLUMN1, COLUMN2, AS_OF_DATE)
                VALUES (:1, :2, :3)
            """
            cursor.execute(sql, (record['COLUMN1'], record['COLUMN2'], as_of_date))

        connection.commit()
        cursor.close()
    except Exception as e:
        print("Error:", e)
        connection.rollback()
    finally:
        connection.close()

    print("Records inserted into the Oracle table.")