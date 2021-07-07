import os
import importlib.util
import logging
import pandas as pd
from datetime import date
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


def get_function(func_module_directory, func):
    file_name, func_name = func.split(".")
    source_file = os.path.join(func_module_directory, f"{file_name}.py")
    spec = importlib.util.spec_from_file_location(func_name, source_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, func_name)


class PythonToPostgresOperator(BaseOperator):
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

        super(PythonToPostgresOperator, self).__init__(task_id=task_id, email=email, default_args=default_args)
        self.conn_id = conn_id
        self.callable = callable
        self.python_dir = python_dir
        self.dependencies = dependencies
        self.table = f"{schema}.{name}"
        self.schema = schema

        self.doc_sql = f"COMMENT ON TABLE {self.table} IS '{description.strip()} Owned by {owner}';"
        for field_name, field_value in fields.items():
            self.doc_sql += f"""COMMENT ON COLUMN {self.table}."{field_name}" IS '{field_value.strip()}';"""

    def get_data(self):
        func = get_function(self.python_dir, self.callable)
        engine = self.get_db_engine()
        if self.dependencies is not None and len(self.dependencies) > 0:
            dependencies = {
                table_name: pd.read_sql_table(table_name, engine, schema=self.schema)
                for table_name in self.dependencies
            }
            return func(**dependencies, db_engine=engine)
        else:
            return func(db_engine=engine)

    def execute(self, context):
        df = self.get_data()
        # add the __view_generated_at column to the df
        if "__view_generated_at" not in df.columns:
            df["__view_generated_at"] = date.today()

        self.copy_to_postgres(df, self.table)
        self.run_sql(self.doc_sql)

    def get_db_engine(self):
        return PostgresHook(postgres_conn_id=self.conn_id).get_sqlalchemy_engine()

    def get_db_connection(self):
        return PostgresHook(postgres_conn_id=self.conn_id).get_conn()

    def copy_to_postgres(self, data_frame, table_name):
        conn = self.get_db_engine()
        schema, table_name = table_name.split(".")
        data_frame.to_sql(
            name=table_name, schema=schema, con=conn, if_exists="replace", index=False
        )

    def run_sql(self, query):
        con = self.get_db_connection()
        try:
            con.cursor().execute(query)
            con.commit()
        except Exception as e:
            logging.error(e)
            con.rollback()
            raise
