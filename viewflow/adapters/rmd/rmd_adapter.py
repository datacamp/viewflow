import os
import airflow

from ..post_execute_monkey_patch import monkey_post_execute

# TODO change viewflow.operators.ecs_operator to Rmd operator

from pathlib import Path
from datetime import timedelta
from airflow.models import BaseOperator
from os import getenv

from typing import Dict, Any


def create_task(parsed_task: Dict[str, Any]):
    
    from airflow.operators.dummy import DummyOperator
    ecs_task = DummyOperator()
    """
    ecs_task = ECSOperator(
        task_id=parsed_task["task_id"],
        email=parsed_task.get("owner"),
        owner=parsed_task.get("owner"),
        task_definition=parsed_task.get("family", "viewflow-view-script"),
        # FIXME: Add cluster and region_name to the DAG config file
        cluster="datacamp-dataeng",
        region_name="us-east-1",
        pool="remote",
        overrides={
            "containerOverrides": [
                {
                    "name": "script",
                    "command": [
                        "Rscript",
                        "--verbose",
                        "-e",
                        "rmarkdown::render('/app/%s', output_file='/tmp/%s.html', run_pandoc = FALSE);"
                        % (parsed_task["task_file_path"], parsed_task["task_id"]),
                    ],
                    "environment": [
                        {"name": "DC_S3_ENVIRONMENT", "value": getenv("ENVIRONMENT")}
                    ],
                }
            ]
        },
        network_configuration={
            "awsvpcConfiguration": {
                "assignPublicIp": "DISABLED",
                "securityGroups": "sg-6405f910,sg-8f3cc7fb".split(","),
                "subnets": "subnet-8cc3a2e8,subnet-e710c1c8".split(","),
            }
        },
        launch_type="FARGATE",
        retry_exponential_backoff=True,
    ) """

    ecs_task.schema_name = parsed_task.get("schema")
    ecs_task.post_execute = monkey_post_execute.__get__(ecs_task, BaseOperator)

    return ecs_task
