FROM apache/airflow:2.5.1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY dags/ /opt/airflow/dags/
COPY entrypoint.sh /opt/airflow/entrypoint.sh

USER root
RUN ["chmod", "+x", "/opt/airflow/entrypoint.sh"]
USER airflow

ENTRYPOINT ["/opt/airflow/entrypoint.sh"]
