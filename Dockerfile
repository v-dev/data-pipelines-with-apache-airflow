FROM apache/airflow:2.0.0-python3.8

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
RUN airflow db init

ENTRYPOINT ["/usr/bin/env", "bash"]
