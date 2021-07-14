import viewflow

from airflow.sensors.external_task_sensor import ExternalTaskSensor
from viewflow.create_dag import ParseContext
from unittest.mock import MagicMock, patch, ANY


def test_parse_external_dependencies():
    parsed = viewflow.parse_dag_dir(
        "./tests/projects/external_deps/dag_2", ParseContext(dag_id="dag_2")
    )
    assert "tasks" in parsed
    tasks = parsed["tasks"]
    for task in tasks:
        if task["task_id"] == "task_2":
            assert len(tasks) == 2
            task_2_deps = task.get("depends_on", [])
            assert len(task_2_deps) == 0
        elif task["task_id"] == "task_3":
            assert len(tasks) == 2
            task_3_deps = task.get("depends_on", [])
            assert len(task_3_deps) == 1
            assert task_3_deps[0].get("dag") == "dag_1"
            assert task_3_deps[0].get("task") == "task_1"


def test_create_external_dependencies():
    mocked_operator = MagicMock()
    dag = viewflow.create_dag(
        "./tests/projects/external_deps/dag_2", {"PostgresOperator": mocked_operator}
    )
    assert "wait_for_dag_1_task_1" in dag.task_dict
    external_sensor = dag.task_dict.get("wait_for_dag_1_task_1")
    assert isinstance(external_sensor, ExternalTaskSensor)
    assert "task_3" in dag.task_dict
    task_3 = dag.task_dict.get("task_3")
    assert task_3._upstream_task_ids == {external_sensor.task_id}

    # assert dag.task_dict.get("task_2")._upstream_task_ids == task2._upstream_task_ids
