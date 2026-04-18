# Base image — official Airflow image with Python 3.12
FROM apache/airflow:slim-3.2.0-python3.12

USER airflow

COPY requirements-airflow.txt .

# Instal dependencies and disable pip-level cache
RUN pip install --no-cache-dir -r requirements-airflow.txt