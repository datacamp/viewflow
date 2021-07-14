import os
from jinja2 import Template
from pathlib import Path
from typing import Dict, Any
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.models import BaseOperator
from ..post_execute_monkey_patch import monkey_post_execute

SQL_TEMPLATE = (
    Path(os.path.dirname(os.path.realpath(__file__))) / "template.sql"
).read_text()


def _get_postgres_operator(parsed_task: Dict[str, Any]) -> PostgresOperator:
    return PostgresOperator(
        sql=SQL_TEMPLATE,
        task_id=parsed_task["task_id"],
        postgres_conn_id=parsed_task.get("connection_id", "postgres_default"),
        email=parsed_task.get("owner"),
        owner=parsed_task.get("owner"),
        params={
            "task_id": parsed_task["task_id"],
            "query": Template(parsed_task["content"]).render(
                params=parsed_task.get("params", {})
            ),
            "fields": parsed_task.get("fields", {}),
            "description": parsed_task["description"],
            "schema": parsed_task.get("schema"),
            "alias": parsed_task.get("alias"),
        },
    )


def create_task(parsed_task: Dict[str, Any]):
    parsed_task["fields"] = {
        key: value.strip() for key, value in parsed_task.get("fields", {}).items()
    }
    parsed_task["description"] = (
        None if "description" not in parsed_task else parsed_task["description"].strip()
    )

    operator = _get_postgres_operator(parsed_task)
    operator.schema_name = parsed_task.get("schema")
    operator.conn_id = parsed_task.get("connection_id")
    operator.post_execute = monkey_post_execute.__get__(operator, BaseOperator)

    return operator
