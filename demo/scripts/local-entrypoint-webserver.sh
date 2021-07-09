#!/usr/bin/env bash

sleep 30

if [[ "$1" == 'Airflow1.10' ]]; then
    airflow initdb
elif [[ "$1" == 'Airflow2' ]]; then
    airflow db init
else
    echo "Invalid argument! Submit a valid Airflow version."
    exit 1
fi

airflow webserver
