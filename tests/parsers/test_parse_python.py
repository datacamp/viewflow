import pandas as pd
import viewflow

from airflow import settings
from airflow.models import TaskInstance, Connection
from airflow.utils import db
from datetime import datetime
from unittest.mock import patch, ANY


def _create_postgres_session():
    session = settings.Session()
    conn = Connection(
        conn_id="postgres_viewflow",
        conn_type="postgres",
        login="user",
        password="passw0rd",
        schema="viewflow",
        host="localhost",
        port=5432,
    )
    db.merge_conn(conn, session)
    return session

@patch("viewflow.operators.python_to_postgres_operator.pd")
@patch("viewflow.adapters.python.python_adapter.PythonToPostgresOperator.copy_to_postgres")
@patch("viewflow.adapters.python.python_adapter.PythonToPostgresOperator.run_sql")
def test_parse_python(run_sql_mock, copy_to_postgres_mock, pandas_mock):
    session = _create_postgres_session()
    pandas_mock.read_sql_table.return_value = pd.DataFrame(
        {"email": ["test@datacamp.com", "test_2@datacamp.com", "what@test.com"]}
    )

    dag = viewflow.create_dag("./tests/projects/python_decorator")
    task = dag.get_task("email_domains")

    ti = TaskInstance(task, datetime(2020, 1, 1))
    
    ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True, session=session)

    copy_to_postgres_mock.assert_called_once_with(ANY, "viewflow.email_domains")
    run_sql_mock.assert_called_once_with(ANY)
    