#!/usr/bin/env bash

sleep 30

airflow initdb
airflow webserver
