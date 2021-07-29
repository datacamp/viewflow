from jinja2 import Template
from pathlib import Path
import os

from airflow.hooks.postgres_hook import PostgresHook # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore


SQL_TEMPLATE = (
    Path(os.path.dirname(os.path.realpath(__file__))) / "template_incremental.sql"
).read_text()

class IncrementalPostgresOperator(PostgresOperator):
    """
    Operator intended to efficiently maintain a materialized view on a regular basis.
    The destination view is not made from scratch every time the operator runs, only recently created/updated rows from the data source are taken into account.
    This is done by running a query with a filter: only rows between the min_time and max_time parameters are selected.

    The destination view is updated by upserting the resulting rows of the query.
    Rows that have been changed (determined based on the primary key) are updated, new rows are inserted.
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

        self.content = content
        self.parameters_initial = params["initial"]
        self.parameters_update = params["update"]

        super().__init__(
            sql=SQL_TEMPLATE,
            task_id=task_id,
            postgres_conn_id=conn_id,
            email=owner,
            owner=owner,
            params={
                "task_id": task_id,
                "fields": fields,
                "description": description,
                "schema": schema,
                "alias": alias,
                "primary_key": primary_key
            },
            default_args=default_args
        )



    def execute(self, context):
        """If the view doesn't yet exist, run the initial query. If it does, run the incremental update."""
        
        # Does table already exist?
        con = PostgresHook(postgres_conn_id=self.conn_id).get_conn()
        con.cursor().execute(f"select exists(select * from information_schema.tables where table_schema='{self.schema}' and table_name='{self.task_id}');")
        table_exists = con.cursor().fetchone()[0]

        # self.params["query"] must be set to self.content where
        # the appropriate parameters (min_time and max_time) are filled out
        if not table_exists:
            # Run with initial parameters
            self.params["query"] = Template(self.content.render(
                params=self.parameters_initial
            ))
        else:
            # Run with update parameters
            self.params["query"] = Template(self.content.render(
                params=self.parameters_update
            ))
        super().execute(context)

