from pathlib import Path
from re import A

from viewflow.parsers.dependencies import get_sql_dependencies
from viewflow.parsers.parse_sql import parse_sql
from viewflow.parsers.parse_yml import parse_yml
from viewflow.create_dag import get_all_dependencies, parse_task_file
from viewflow.create_dag import ParseContext


sql_file = Path("./tests/projects/sql/task_1.sql")
task_parsed = parse_sql(sql_file)
task_parsed["dag"] = "dag_1"

def test_get_sql_dependencies():
    sql_query = task_parsed["content"]
    schema_name = task_parsed["schema"]

    dependencies = get_sql_dependencies(sql_query, schema_name)

    expected_dependencies = ["courses", "exercises"]

    for dependency in expected_dependencies:
        assert dependency in dependencies


def test_get_dependencies_none():
    wrong_schema_name = "NOT" + task_parsed["schema"]
    dependencies = get_all_dependencies(task_parsed, wrong_schema_name)
    expected_dependencies = []

    for dependency in expected_dependencies:
        assert dependency in dependencies


def test_get_dependencies_external_internal():
    yml_file = Path("tests/projects/external_deps/dag_2/task_3.yml")
    task_parsed = parse_task_file(yml_file, ParseContext(dag_id="dag_2"))

    expected_dependencies = [
        {"task": "task_1", "dag": "dag_1"},
    ]

    for dependency in expected_dependencies:
        assert dependency in task_parsed["depends_on"]
