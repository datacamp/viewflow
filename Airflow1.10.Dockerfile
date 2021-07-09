FROM apache/airflow:1.10.14-python3.7

USER root
RUN apt-get update -yqq \
    && apt-get install -y libpq-dev \
    && apt-get install -y build-essential \
    && apt-get install -y vim \
    && apt-get install -y git 

RUN pip install --upgrade pip

# Symbolic link necessary for the apache-airflow-backport-providers-* packages
RUN ln -s /usr/local/lib/python3.7/site-packages/airflow/providers /home/airflow/.local/lib/python3.7/site-packages/airflow/

COPY ./Airflow@1.10/requirements.txt /requirements.txt
#RUN pip install -r /requirements.txt

COPY ./viewflow /viewflow/viewflow
COPY ./Airflow@1.10/pyproject.toml /viewflow/
COPY ./README.md /viewflow/
COPY ./demo /viewflow/demo

RUN pip install /viewflow

USER airflow
