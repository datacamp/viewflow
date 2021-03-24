from airflow.hooks.postgres_hook import PostgresHook
import psycopg2
import logging


def monkey_post_execute(self, context, result=None):
    """Monkey patch function that adds the __view_generated_at column to all views"""
    hook = PostgresHook(postgres_conn_id=self.conn_id)
    view_name = context["task"].task_id

    sql_exists = (
        f"SELECT __view_generated_at FROM {self.schema_name}.{view_name} LIMIT 1"
    )
    try:
        hook.run(sql_exists)
    except psycopg2.errors.UndefinedColumn as e:
        sql_add_col = f"ALTER TABLE {self.schema_name}.{view_name} ADD COLUMN __view_generated_at DATE DEFAULT CURRENT_DATE;"
        try:
            hook.run(sql_add_col)
        except Exception as e:
            logging.error(
                "An error occurred while adding the __view_generated_at column."
            )
            raise
    except psycopg2.errors.UndefinedTable as e:
        logging.warning(f"WARNING: It seems that the view {view_name} doesn't exist")
