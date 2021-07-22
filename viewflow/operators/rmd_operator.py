import os
from pathlib import Path
from typing import List
from textwrap import dedent
import re
from viewflow.operators.r_operator import ROperator

from sqlalchemy.inspection import inspect
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.engine import Engine
from viewflow.parsers.dependencies import get_r_dependencies

class RmdOperator(ROperator):

    def __init__(
        self,
        conn_id,
        task_id,
        email,
        description,
        fields,
        content,
        owner,
        schema,
        dependency_function,
        default_args={}
    ):

        self.conn_id = conn_id
        self.task_id = task_id
        self.schema = schema
        self.table = f"{schema}.{self.task_id}"
        self.dependency_function = dependency_function

        self.content = content
        self.r_content = extractR(content)
        Rmd_full_script = self.generateFullScript()
        file_name = self.saveFullScript(Rmd_full_script)

        super(ROperator, self).__init__(
            bash_command=f"Rscript -e \"rmarkdown::render('{file_name}', run_pandoc=FALSE)\"",
            task_id=task_id,
            email=email,
            default_args=default_args
        )

        self.doc_sql = f"COMMENT ON TABLE {self.table} IS '{description.strip()}\nOwned by {owner}';"
        for field_name, field_value in fields.items():
            self.doc_sql += f"""COMMENT ON COLUMN {self.table}."{field_name}" IS '{field_value.strip()}';"""


    def generateFullScript(self) -> str:
        """Extend user-provided Rmd script to the full script.
        The full script is composed of the following parts:
            1) Connecting to the database and reading the tables the script depends on
            2) The user-provided script which creates a new table
            3) Materializing the new table in the database"""
        
        # Connecting to the database
        conn = self.get_db_connection()
        Rmd_script = dedent(f"""
        ```{{r, include=FALSE}}
        library(DBI)
        conn <- dbConnect(RPostgres::Postgres(),
            dbname = '{conn.info.dbname}', 
            host = '{conn.info.host}',
            port = {conn.info.port},
            user = '{conn.info.user}',
            password = '{conn.info.password}',
        )
        """)

        # Reading the necessary tables for each schema
        pg_engine: Engine = self.get_db_engine()
        pg_inspector: Inspector = inspect(pg_engine)
        schema_names: List[str] = pg_inspector.get_schema_names()
        for schema in schema_names:
            dependencies = get_r_dependencies(self.r_content, schema, self.dependency_function)
            for script_name, table_name in dependencies.items():
                Rmd_script += f"{script_name} <- dbReadTable(conn, name = Id(schema = '{schema}', table = '{table_name}'))\n"
        Rmd_script += "```\n"

        # The user-provided script which creates a new table named self.task_id
        Rmd_script += self.content

        # Materializing the new table in the database
        Rmd_script += dedent(f"""
        ```{{r, include=FALSE}}
        dbWriteTable(conn, name = Id(schema = '{self.schema}', table = '{self.task_id}'), {self.task_id}, overwrite=TRUE)
        dbDisconnect(conn)
        ```
        """)
        return Rmd_script
    

    def saveFullScript(self, full_script):
        """Save full_script to file and return the filename"""
        folder = os.environ["AIRFLOW_HOME"] + "/data"
        file_name = folder + f"/{self.schema}.{self.task_id}_GENERATED.Rmd"
        Path(folder).mkdir(exist_ok=True)
        with open(file_name, "w") as f:
            f.write(full_script)
        return file_name

    

def extractR(rmd_content):
    """Extract the actual R code from the given Rmd script"""
    return "\n".join(re.findall(r"^```\{r[ \}].*?$(.+?)^```", rmd_content, flags=re.MULTILINE|re.DOTALL))
