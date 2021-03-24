import viewflow

from airflow import DAG
from unittest.mock import MagicMock, patch, ANY
from pathlib import Path
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator
from viewflow.create_dag import ParseContext


def test_parse_dag_dir():
    parsed = viewflow.parse_dag_dir(
        "./tests/projects/dag1_simple", ParseContext(dag_id="dag1_simple")
    )

    expected = {
        "dag_config": {
            "default_args": {"owner": "data@datacamp.com"},
            "schedule_interval": "0 5 * * *",
            "start_date": datetime.strptime("2020-04-29", "%Y-%m-%d"),
        },
        "tasks": [
            {
                "owner": "engineering@datacamp.com",
                "type": "PostgresOperator",
                "description": "This is a very simple task.",
                "fields": {"id": "This is the ID."},
                "tags": ["cool", "new"],
                "content": "SELECT test FROM some_table",
                "task_id": "task_1",
                "task_file_path": "dag1_simple/task_1.yml",
                "schema": "viewflow",
            },
            {
                "owner": "engineering@datacamp.com",
                "type": "PostgresOperator",
                "email": "engineering@datacamp.com",
                "description": "This is a very simple task with dependency.",
                "depends_on": [{"task": "task_1", "dag": "dag1_simple"}],
                "fields": {"id": "This is the ID."},
                "tags": ["cool", "new"],
                "content": "SELECT id FROM viewflow.task_1 WHERE id=0",
                "task_id": "task_2",
                "schema": "viewflow",
                "task_file_path": "dag1_simple/task_2.yml",
            },
        ],
    }
    assert parsed == expected


def test_create_dag():
    mocked_operator = MagicMock()
    dag = viewflow.create_dag(
        "./tests/projects/dag1_simple", {"PostgresOperator": mocked_operator}
    )
    expected_dag = DAG(
        "dag1_simple",
        default_args={"owner": "data@datacamp.com"},
        schedule_interval="0 5 * * *",
        start_date=datetime.strptime("2020-04-29", "%Y-%m-%d"),
    )
    with expected_dag:
        task1 = DummyOperator(task_id="task_1")
        task2 = DummyOperator(task_id="task_2")
        task1 >> task2

    assert dags_equal(dag, expected_dag)
    assert dag.task_dict.get("task_2")._upstream_task_ids == task2._upstream_task_ids


@patch("viewflow.adapters.postgresql.postgres_adapter.PostgresOperator")
def test_create_task(postgres_operator_mock):
    task_parsed = viewflow.parse_task_file(
        Path("./tests/projects/dag1_simple/task_1.yml"),
        ParseContext(dag_id="dag1_simple"),
    )
    viewflow.create_task(task_parsed)
    postgres_operator_mock.assert_called_once_with(
        sql=ANY,
        task_id="task_1",
        postgres_conn_id="postgres_default",
        owner="engineering@datacamp.com",
        email=ANY,
        params={
            "task_id": "task_1",
            "query": "SELECT test FROM some_table",
            "fields": {"id": "This is the ID."},
            "description": "This is a very simple task.",
            "schema": "viewflow",
            "alias": None,
        },
    )


# https://airflow.apache.org/docs/stable/_modules/airflow/models/dag.html#DAG.__eq__
def dags_equal(dag1, dag2):
    if type(dag1) == type(dag2) and dag1.dag_id == dag2.dag_id:
        return all(
            getattr(dag1, c, None) == getattr(dag2, c, None)
            for c in set(dag1._comps) - {"last_loaded"}
        )
    return False
