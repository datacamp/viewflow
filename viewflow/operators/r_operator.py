import logging
import re
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash_operator import BashOperator


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
        dependencies,
        default_args={}
    ):

        self.conn_id = conn_id
        self.content = content
        self.dependencies = dependencies
        self.schema = schema
        self.table_name = re.sub(r".[rR]$", "", task_file_path.split("/")[-1])
        self.table = f"{schema}.{self.table_name}"

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


    def generateFullScript(self):
        """Extend user-provided R script to the full script.
        The full script is composed of the following parts:
            1) Connecting to the database and reading the tables the script depends on
            2) The user-provided script which creates a new table
            3) Materializing the new table in the database"""
        
        conn = self.get_db_connection()

        R_make_connection = f'''
        conn <- dbConnect(RPostgres::Postgres(),
            dbname = {conn.info.dbname}, 
            host = 'localhost',
            port = {conn.info.port},
            user = {conn.info.user},
            password = {conn.info.password},
        )
        '''
        
        full_script = self.content
        return re.sub("'", "\"", full_script)


    def execute(self, context):
        super().execute(context)
        self.run_sql(self.doc_sql)

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
