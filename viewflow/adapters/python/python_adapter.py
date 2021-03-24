from viewflow.operators.python_to_postgres_operator import PythonToPostgresOperator
from typing import Any, Dict
from viewflow.operators.python_to_postgres_operator import PythonToPostgresOperator
import pandas as pd


class DependantDataFrame(pd.DataFrame):
    pass


def create_task(parsed_task: Dict[str, Any]) -> PythonToPostgresOperator:
    dependencies = None
    if parsed_task.get("inject_dependencies", False):
        dependencies = [
            table_name for table_name in parsed_task.get("dependencies", [])
        ]

    return PythonToPostgresOperator(
        conn_id=parsed_task["connection_id"],
        callable=parsed_task["callable"],
        python_dir=parsed_task["python_dir"],
        description=parsed_task["description"],
        fields=parsed_task.get("fields", {}),
        name=parsed_task["name"],
        owner=parsed_task["owner"],
        task_id=parsed_task["task_id"],
        schema=parsed_task.get("schema"),
        email=parsed_task.get("owner"),
        dependencies=dependencies,
    )
