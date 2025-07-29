

FROM apache/airflow:3.0.3-python3.12
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /

RUN python -m ensurepip --upgrade
RUN pip install --no-cache-dir "apache-airflow==3.0.3" -r /requirements.txt


COPY --chown=airflow:root /dags/etl_dag.py /opt/airflow/dags
