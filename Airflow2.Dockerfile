FROM apache/airflow:2.1.1-python3.7

USER root
RUN apt-get update -yqq \
    && apt-get install -y libpq-dev \
    && apt-get install -y build-essential \
    && apt-get install -y vim \
    && apt-get install -y git 

RUN pip install --upgrade pip

COPY ./Airflow@2/requirements.txt /requirements.txt
#RUN pip install -r /requirements.txt

COPY ./viewflow /viewflow/viewflow
COPY ./Airflow@2/pyproject.toml /viewflow/
COPY ./README.md /viewflow/
COPY ./demo /viewflow/demo

RUN pip install /viewflow

USER airflow