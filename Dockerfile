FROM apache/airflow:2.8.0

ADD webserver_config.py /opt/airflow/webserver_config.py

USER airflow

COPY requirements.txt .
COPY credentials.env .

RUN pip install -r requirements.txt
