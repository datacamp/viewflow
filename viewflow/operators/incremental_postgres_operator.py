from jinja2 import Template
from pathlib import Path
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore


SQL_TEMPLATE = (
    Path(os.path.dirname(os.path.realpath(__file__))) / "template_incremental.sql"
).read_text()

class IncrementalPostgresOperator(PostgresOperator):
    """
    Operator intended to efficiently update a materialized view on a regular basis.
    The destination view is not made from scratch every time the operator runs, only recently inserted/updated rows from the data source are taken into account.
    This is done by running a query with a filter: only rows between the min_time and max_time parameters are selected.

    The destination view is updated by upserting the resulting rows of the query.
    Rows that have been changed (determined based on the primary key specified in the yml file) are updated, new rows are inserted.
    """

    def __init__(
        self,
        conn_id,
        task_id,
        description,
        content,
        owner,
        schema,
        params,
        primary_key,
        fields,
        alias,
        default_args={}
    ):

        super().__init__(
            sql="", # To be determined in self.execute
            task_id=task_id,
            postgres_conn_id=conn_id,
            email=owner,
            owner=owner,
            default_args=default_args
        )

        self.conn_id = conn_id
        self.task_id = task_id
        self.description = description
        self.content = content
        self.owner = owner
        self.schema = schema
        self.parameters_time = params
        self.primary_key = primary_key
        self.fields = fields
        self.alias = alias


    def get_query(self):
        """Return the query that selects the new rows for the target view.
        If the target view doesn't yet exist, use the initial parameters. If it does, use the update parameters."""

        # Does table already exist?
        with PostgresHook(postgres_conn_id=self.conn_id).get_conn().cursor() as cur:
            cur.execute(f"select exists(select * from information_schema.tables where table_schema='{self.schema}' and table_name='{self.task_id}');")
            table_exists = cur.fetchone()[0]

        # The result is equal to self.content where the appropriate parameters (min_time and max_time) are filled out.
        return Template(self.content).render(
            min_time=self.parameters_time["update" if table_exists else "initial"]["min_time"],
            max_time=self.parameters_time["update" if table_exists else "initial"]["max_time"]
        )


    def execute(self, context):
        """If the view doesn't yet exist, run the initial query. If it does, run the incremental update."""
        
        self.sql = Template(SQL_TEMPLATE).render(params={
            "task_id": self.task_id,
            "fields": self.fields,
            "description": self.description,
            "schema": self.schema,
            "alias": self.alias,
            "primary_key": self.primary_key,
            "query": self.get_query()
        })

        super().execute(context)
