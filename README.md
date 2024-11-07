# Airflow Example Repository

This repository contains a setup for Apache Airflow with Docker Compose, including example DAGs and scripts for task execution.

## Prerequisites

- Docker
- Docker Compose

## Repository Structure

```
airflow_example_repo/
├── dags/
│   ├── basic_dag.py
│   ├── dependent_dag.py
│   ├── advanced_dag.py
│   └── scripts/
│       └── task_functions.py
├── Dockerfile
├── docker-compose.yml
├── entrypoint.sh
├── requirements.txt
└── README.md
```

## Setup Instructions

1. **Clone the Repository**

   ```bash
   git clone <repository_url>
   cd airflow_example_repo
   ```

2. **Build the Docker Images**

   ```bash
   chmod +x entrypoint.sh //only necessary if on *nix machine
   docker-compose build
   ```

3. **Start Airflow Services**

   ```bash
   docker-compose up -d
   ```

4. **Access the Airflow UI**

   Open your browser and go to `http://localhost:8080`. Log in with the default admin credentials (`admin` / `admin`).

## Running the DAGs

1. **Add Your DAGs**

   Ensure your DAG files are located in the `dags/` directory.

2. **Trigger a DAG**

   To manually trigger a DAG:
   - Go to the Airflow UI.
   - Navigate to the "DAGs" tab.
   - Turn on the DAG toggle for the DAG you want to trigger.
   - Click the "Trigger DAG" button next to the DAG.

3. **Monitor DAG Runs**

   Monitor the progress and status of your DAG runs in the Airflow UI. Logs for each task can be viewed by clicking on the task instance.

## Troubleshooting

### Logs

Check the logs for the webserver, scheduler, and other services if you encounter issues:

```bash
docker-compose logs webserver
docker-compose logs scheduler
```

## Conclusion

This setup provides a basic environment for running and managing Airflow DAGs using Docker Compose. Customize the `dags/` directory and other configurations as needed for your use case. For more information on Airflow, visit the [Apache Airflow Documentation](https://airflow.apache.org/docs/apache-airflow/stable/).
