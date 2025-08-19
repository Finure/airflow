ARG AIRFLOW_VERSION=3.0.1

FROM apache/airflow:${AIRFLOW_VERSION}
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt