import pytest
from pathlib import Path
from viewflow.parsers.parse_sql import parse_sql


def test_parse_sql():
    sql_file = Path("./tests/projects/sql/task_1.sql")
    task_parsed = parse_sql(sql_file)
    expected = {
        "owner": "engineering@datacamp.com",
        "description": "Description",
        "fields": {
            "course_id": "The id of the course",
            "title": "The title of the course",
            "technology": "The technology of the course",
        },
        "type": "PostgresOperator",
        "content": "\nSELECT course_id, title, technology, exercise_title\n  FROM viewflow.courses c\n  JOIN viewflow.exercises e using (course_id) \n",
        "task_file_path": str(sql_file),
        "schema": "viewflow",
    }
    assert task_parsed == expected
