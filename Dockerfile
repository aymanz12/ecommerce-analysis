FROM apache/airflow:2.1.1

COPY requirements.txt /requirements.txt

RUN pip install --user -r /requirements.txt

