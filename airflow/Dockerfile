# Local Airflow image for orchestrating the OpenAQ pipeline.
# Slim base ships no providers; install only the two the DAG needs, pinned
# against the official Airflow 2.9.3 constraints for Python 3.11.
# DockerOperator talks to the host daemon over /var/run/docker.sock via the
# docker Python SDK (bundled with the docker provider) — no docker CLI needed.
FROM apache/airflow:slim-2.9.3-python3.11

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow==2.9.3" \
    "apache-airflow-providers-docker" \
    "apache-airflow-providers-databricks" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
