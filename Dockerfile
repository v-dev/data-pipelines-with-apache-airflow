FROM apache/airflow:2.0.0-python3.8

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
RUN airflow db init

USER root
COPY bash_aliases /home/airflow/.bash_aliases
RUN chown -R airflow:airflow /home/airflow/.bash_aliases

USER airflow

ENTRYPOINT ["/bin/bash"]
