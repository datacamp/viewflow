import os
import yaml
import logging
import pickle
import re
import networkx as nx
import random
from collections import namedtuple
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, Any, TypeVar
from airflow import DAG  # type: ignore
from airflow.models import BaseOperator  # type: ignore
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from .adapters.postgresql import postgres_adapter
from .adapters.python import python_adapter
from .parsers.parse_yml import parse_yml
from .parsers.parse_sql import parse_sql
from .parsers.parse_python import parse_python
from .parsers.dependencies import get_sql_dependencies
from .parsers.dependencies import get_python_dependencies


O = TypeVar("O", bound=BaseOperator)

DAG_CONFIG_FILE = "config.yml"
OPERATORS = {
    "PostgresOperator": postgres_adapter.create_task,
    "PythonToPostgresOperator": python_adapter.create_task,
}

PARSERS = {".yml": parse_yml, ".sql": parse_sql, ".py": parse_python}

SQL_OPERATORS = ["PostgresOperator"]


@dataclass
class ParseContext:
    dag_id: str


def parse_dag_dir(
    input_dir: str, parse_context: ParseContext, dag_config_file=DAG_CONFIG_FILE
) -> Dict[str, Any]:
    dag_dir = Path(input_dir)
    dag_config_file_path = dag_dir / dag_config_file
    dag_config = yaml.safe_load(dag_config_file_path.read_text())
    dag_config["start_date"] = datetime.strptime(dag_config["start_date"], "%Y-%m-%d")
    task_files = sorted(os.listdir(dag_dir))

    return {
        "dag_config": dag_config,
        "tasks": [
            task
            for task in [
                parse_task_file(dag_dir / task_file, parse_context)
                for task_file in task_files
                if task_file != dag_config_file
            ]
            if task is not None
        ],
    }


def parse_depends_on_entry(depends_on_entry: str, dag_name: str) -> Dict[str, Any]:
    match = re.search("^(.*?)/(.*?)$", depends_on_entry)
    if match is None:
        return {"task": depends_on_entry, "dag": dag_name}
    dag, task = match.groups()
    return {"task": task, "dag": dag}


def parse_depends_on(depends_on, dag_name: str):
    if not (isinstance(depends_on, list)):
        raise TypeError("'depends_on' should be a list")
    return [
        parse_depends_on_entry(depends_on_entry, dag_name)
        for depends_on_entry in depends_on
    ]


def parse_task_file(
    task_file_path: Path, parse_context: ParseContext
) -> Dict[str, Any]:

    parser = PARSERS.get(task_file_path.suffix.lower())

    if parser is None:
        return None

    task_config = parser(task_file_path)

    task_config["task_id"] = task_file_path.stem
    task_config["task_file_path"] = f"{parse_context.dag_id}/{task_file_path.name}"
    if "depends_on" in task_config:
        task_config["depends_on"] = parse_depends_on(
            task_config["depends_on"], parse_context.dag_id
        )

    return task_config


def get_all_dependencies(task, schema_name):
    if task["type"] == "PostgresOperator":
        dependencies = []
        dependencies.extend(
            get_sql_dependencies(task["content"], schema_name, task["dag"])
        )
        dependencies = [x["task"] for x in dependencies]
    elif task["type"] == "PythonToPostgresOperator":
        dependencies = get_python_dependencies(
            task["content"], schema_name, task["dag"]
        )
        dependencies = [x["task"] for x in dependencies]

    else:
        dependencies = []
    return dependencies


def create_dag_from_config(dag_id: str, dag_config, operators=OPERATORS) -> DAG:
    dag = DAG(dag_id, **dag_config["dag_config"])
    with dag:
        task_dict = {}
        for task in dag_config["tasks"]:
            created_task = create_task(task, operators)
            if created_task == None:
                continue
            task_dict[task.get("task_id")] = {
                "task": created_task,
                "dependencies": task.get("depends_on", []),
            }

        external_dependency_sensors = {}
        for task_id, task_and_dependencies in task_dict.items():
            task = task_and_dependencies.get("task")
            dependencies = task_and_dependencies.get("dependencies", [])

            for dependency in dependencies:
                dependency_dag_id = dependency.get("dag")
                dependency_task_id = dependency.get("task")

                try:
                    if dependency_dag_id == dag_id:
                        task_dict[dependency_task_id]["task"] >> task
                    else:
                        external_dependency_task_id = (
                            f"wait_for_{dependency_dag_id}_{dependency_task_id}"
                        )

                        if external_dependency_task_id in external_dependency_sensors:
                            external_task_sensor = external_dependency_sensors[
                                external_dependency_task_id
                            ]
                        else:
                            external_task_sensor = ExternalTaskSensor(
                                dag=dag,
                                task_id=f"wait_for_{dependency_dag_id}_{dependency_task_id}",
                                external_dag_id=dependency_dag_id,
                                external_task_id=dependency_task_id,
                                allowed_states=["success"],
                                mode="reschedule",
                                poke_interval=random.randint(150, 210),
                            )
                            external_dependency_sensors[
                                external_dependency_task_id
                            ] = external_task_sensor

                        external_task_sensor >> task
                except KeyError as e:
                    logging.error(
                        f"Cannot set the task {task_id} dependencies ({str(e)})"
                    )
    return dag


def create_dag(input_dir: str, operators=OPERATORS) -> DAG:
    dag_dir = Path(input_dir)
    dag_id = dag_dir.name

    parse_context = ParseContext(dag_id=dag_id)
    dag_config = parse_dag_dir(input_dir, parse_context=parse_context)
    return create_dag_from_config(dag_id, dag_config, OPERATORS)


def create_task(parsed_task: Dict[str, Any], operators=OPERATORS) -> O:
    try:
        adapter = operators[parsed_task["type"]]
    except KeyError as e:
        logging.error(
            f"Error while retrieving the adapter {parsed_task['type']}: {str(e)}"
        )
        return None
    task = adapter(parsed_task)

    return task


Dependency = namedtuple("Dependency", ["task", "dag"])


def enrich_dags_config(dags_parsed, schema_name):
    """Enrich DAGs configuration with dependencies"""
    tasks = []
    for dag_parsed in dags_parsed:
        for task in dag_parsed["dag_config"]["tasks"]:
            tasks.append(
                {
                    "dag": dag_parsed["dag_name"],
                    "task_id": task["task_id"],
                    "type": task["type"],
                    "content": task.get("content", ""),
                    "disable_implicit_dependencies": task.get(
                        "disable_implicit_dependencies"
                    ),
                }
            )
    for task in tasks:
        task["depends_on"] = get_all_dependencies(task, schema_name)
    task_dags = {task["task_id"]: task["dag"] for task in tasks}
    for task in tasks:
        task["depends_on"] = [
            {"task": dep, "dag": task_dags[dep]}
            for dep in task["depends_on"]
            if dep in task_dags
        ]
    task_dependencies = {task["task_id"]: task["depends_on"] for task in tasks}
    for dag_parsed in dags_parsed:
        for task in dag_parsed["dag_config"]["tasks"]:
            if task.get("disable_implicit_dependencies") is True:
                task["depends_on"] = []
                continue
            explicit_dependencies = set(
                Dependency(**entry) for entry in task.get("depends_on", [])
            )
            automatic_dependencies = set(
                Dependency(**entry) for entry in task_dependencies[task["task_id"]]
            )
            all_dependencies = [
                dict(dep._asdict())
                for dep in explicit_dependencies | automatic_dependencies
            ]
            task["depends_on"] = all_dependencies

    return dags_parsed


def parse_dags_dir(dags_dir: str):
    dags_config = []
    dags_dir_path = Path(dags_dir)
    dags = [
        dag
        for dag in dags_dir_path.iterdir()
        if dag.is_dir() and Path(f"{dag}/config.yml").is_file()
    ]
    for input_dir in dags:
        dag_dir = Path(input_dir)
        dag_id = dag_dir.name
        parse_context = ParseContext(dag_id=dag_id)
        try:
            dag_config = parse_dag_dir(input_dir, parse_context=parse_context)
            dags_config.append({"dag_name": dag_id, "dag_config": dag_config})
        except Exception as error:
            logging.error(f"Error for dir: {input_dir}\n{error}")
            continue
    return dags_config


def create_airflow_graph(dags_enriched):
    nodes = []
    edges = []
    for dag_enriched in dags_enriched:
        dag_id = dag_enriched["dag_name"]
        for task in dag_enriched["dag_config"]["tasks"]:
            task_label = f"{dag_id}/{task['task_id']}"
            nodes.append(task_label)
            for dependency in task.get("depends_on", []):
                dependency_label = f"{dependency['dag']}/{dependency['task']}"
                edges.append((dependency_label, task_label))
    graph = nx.DiGraph()
    graph.add_nodes_from(nodes)
    graph.add_edges_from(edges)
    return graph


def is_acyclic(dags_enriched):
    G = create_airflow_graph(dags_enriched)
    cycles = list(nx.simple_cycles(G))
    if len(cycles) > 0:
        logging.warning("The DAGs have cycles ...")
        logging.warning(cycles)
        return False
    else:
        return True


def create_dags(input_dir: str, dag_scope: Dict[str, Any], views_schema: str):
    """Create a set of dags from a directory of DAGs"""
    dags_parsed = parse_dags_dir(input_dir)
    dags_enriched = enrich_dags_config(dags_parsed, views_schema)
    for dag_enriched in dags_enriched:
        dag_scope[dag_enriched["dag_name"]] = create_dag_from_config(
            dag_enriched["dag_name"], dag_enriched["dag_config"]
        )


def create_dags_from_compiled_file(
    compiled_dags_file_name: str, dag_scope: Dict[str, Any]
):
    with open(compiled_dags_file_name, "rb") as compiled_dags_file:
        dags_enriched = pickle.load(compiled_dags_file)
    for dag_enriched in dags_enriched:
        dag_scope[dag_enriched["dag_name"]] = create_dag_from_config(
            dag_enriched["dag_name"], dag_enriched["dag_config"]
        )


def compile_dags(input_dir: str, output_file_name: str, views_schema: str):
    dags_parsed = parse_dags_dir(input_dir)
    dags_enriched = enrich_dags_config(dags_parsed, views_schema)
    dag_is_acyclic = is_acyclic(dags_enriched)
    if not dag_is_acyclic:
        raise ValueError("The dags graph has cycles. Please remove them.")
    with open(output_file_name, "wb") as output_file:
        pickle.dump(dags_enriched, output_file)
