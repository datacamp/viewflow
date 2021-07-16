from typing import Dict, Any

from viewflow.operators.r_operator import ROperator # type: ignore
from airflow.models import BaseOperator # type: ignore
from ..post_execute_monkey_patch import monkey_post_execute


def create_task(parsed_task: Dict[str, Any]):
    parsed_task["fields"] = {
        key: value.strip() for key, value in parsed_task.get("fields", {}).items()
    }
    parsed_task["description"] = (
        None if "description" not in parsed_task else parsed_task["description"].strip()
    )

    operator = ROperator(parsed_task)
    operator.schema_name = parsed_task.get("schema")
    operator.conn_id = parsed_task.get("connection_id")
    operator.post_execute = monkey_post_execute.__get__(operator, BaseOperator)

    return operator
