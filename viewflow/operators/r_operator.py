import os
import logging
import pandas as pd
from datetime import date
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
from rpy2.rinterface import RRuntimeError

class ROperator(BaseOperator):

    def __init__(
        self,
        conn_id,
        task_id,
        email,
        callable,
        python_dir,
        description,
        fields,
        name,
        owner,
        schema,
        dependencies,
        default_args={}
    ):

        super(ROperator, self).__init__(task_id=task_id, email=email, default_args=default_args)
        self.conn_id = conn_id
        self.callable = callable
        self.python_dir = python_dir
        self.dependencies = dependencies
        self.table = f"{schema}.{name}"
        self.schema = schema

        self.doc_sql = f"COMMENT ON TABLE {self.table} IS '{description.strip()} Owned by {owner}';"
        for field_name, field_value in fields.items():
            self.doc_sql += f"""COMMENT ON COLUMN {self.table}."{field_name}" IS '{field_value.strip()}';"""

