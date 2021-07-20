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

    operator = ROperator(
        conn_id=parsed_task["connection_id"],
        task_id=parsed_task["task_id"],
        email=parsed_task.get("owner"),
        description= parsed_task["description"],
        fields=parsed_task["fields"],
        task_file_path=parsed_task["task_file_path"],
        content=parsed_task["content"],
        owner=parsed_task.get("owner"),
        schema=parsed_task["schema"],
        dependency_function=parsed_task.get("dependency_function")
    )
    operator.schema_name = parsed_task.get("schema")
    operator.conn_id = parsed_task.get("connection_id")
    operator.post_execute = monkey_post_execute.__get__(operator, BaseOperator)

    return operator
