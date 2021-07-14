#!/usr/bin/env bash

sleep 30

if [[ "$1" == 'Airflow1.10' ]]; then
    airflow initdb
elif [[ "$1" == 'Airflow2' ]]; then
    airflow db init
    # https://airflow.apache.org/docs/apache-airflow/stable/security/webserver.html#id4
    echo "AUTH_ROLE_PUBLIC = 'Admin'" >> $AIRFLOW_HOME/webserver_config.py
else
    echo "Invalid argument! Submit a valid Airflow version."
    exit 1
fi

airflow webserver
