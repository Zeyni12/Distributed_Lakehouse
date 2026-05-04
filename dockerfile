FROM apache/airflow:3.2.1
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt