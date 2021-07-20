import logging
import re
from typing import List
from textwrap import dedent
from airflow.providers.postgres.hooks.postgres import PostgresHook
try:
    from airflow.operators.bash import BashOperator # Airflow version >= 2
except ModuleNotFoundError:
    from airflow.operators.bash_operator import BashOperator

from sqlalchemy.inspection import inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine import Engine
from viewflow.parsers.dependencies import get_r_dependencies

class ROperator(BashOperator):

    def __init__(
        self,
        conn_id,
        task_id,
        email,
        description,
        fields,
        task_file_path,
        content,
        owner,
        schema,
        dependency_function,
        default_args={}
    ):

        self.conn_id = conn_id
        self.task_id = task_id
        self.content = content
        self.schema = schema
        self.table_name = re.sub(r".[rR]$", "", task_file_path.split("/")[-1])
        self.table = f"{schema}.{self.table_name}"
        self.dependency_function = dependency_function

        R_full_script = self.generateFullScript()

        super().__init__(
            bash_command="Rscript -e '" + R_full_script + "'",
            task_id=task_id,
            email=email,
            default_args=default_args
        )

        self.doc_sql = f"COMMENT ON TABLE {self.table} IS '{description.strip()}\nOwned by {owner}';"
        for field_name, field_value in fields.items():
            self.doc_sql += f"""COMMENT ON COLUMN {self.table}."{field_name}" IS '{field_value.strip()}';"""


    def generateFullScript(self) -> str:
        """Extend user-provided R script to the full script.
        The full script is composed of the following parts:
            1) Connecting to the database and reading the tables the script depends on
            2) The user-provided script which creates a new table
            3) Materializing the new table in the database"""
        
        # TODO use self.content to determine which tables this script depends on
        # first use conn to list all schema names?
        # Then use get_r_dependencies to determine dependencies for each schema

        conn = self.get_db_connection()
        R_script = dedent(f'''
        library(DBI)
        conn <- dbConnect(RPostgres::Postgres(),
            dbname = '{conn.info.dbname}', 
            host = 'localhost',
            port = {conn.info.port},
            user = '{conn.info.user}',
            password = '{conn.info.password}',
        )
        ''')

        pg_engine: Engine = self.get_db_engine()
        pg_inspector: Inspector = inspect(pg_engine)
        schema_names: List[str] = pg_inspector.get_schema_names()
        # Read necessary tables for each schema
        for schema in schema_names:
            dependencies = get_r_dependencies(self.content, self.schema, self.dependency_function)
            for script_name, table_name in dependencies.items():
                R_script += f"{script_name} = dbReadTable(conn, name = Id(schema = '{schema}', table = '{table_name}'))\n"


        full_script = self.content
        return re.sub("'", "\"", full_script)


    def execute(self, context):
        super().execute(context)
        self.run_sql(self.doc_sql)
    
    def get_db_engine(self):
        return PostgresHook(postgres_conn_id=self.conn_id).get_sqlalchemy_engine()

    def get_db_connection(self):
        return PostgresHook(postgres_conn_id=self.conn_id).get_conn()
    
    def run_sql(self, query):
        con = self.get_db_connection()
        try:
            con.cursor().execute(query)
            con.commit()
        except Exception as e:
            logging.error(e)
            con.rollback()
            raise
