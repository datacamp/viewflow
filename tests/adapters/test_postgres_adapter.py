from datetime import datetime
import pytest
from pathlib import Path
from unittest.mock import patch, ANY, MagicMock, Mock
import jinja2
import viewflow
from viewflow.create_dag import ParseContext

from airflow import DAG
from airflow.models import TaskInstance, Connection
from airflow.hooks.postgres_hook import PostgresHook
from airflow import settings
from airflow.utils import db


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


def test_postgres_adapter(monkeypatch):
    session = _create_postgres_session()

    dag = viewflow.create_dag("./tests/projects/postgresql/simple_dag")
    task = dag.get_task("task_1")
    ti = TaskInstance(task, datetime(2020, 1, 1))

    with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS viewflow.task_1")

    ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True, session=session)

    with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM viewflow.task_1")
            (count,) = cur.fetchone()
            assert count == 8
            cur.execute("SELECT __view_generated_at FROM viewflow.task_1")
            res = cur.fetchone()
            assert len(res) == 1


def test_postgres_with_alias():
    params_mock = Mock()
    params_mock.params = {
        "task_id": "task_1",
        "query": "SELECT user_id, email FROM main_app.users limit 1",
        "fields": {},
        "description": "This is a very simple Postgres task  with an alias",
        "schema": "viewflow",
        "alias": "task_1_aliased",
    }

    with open("./viewflow//adapters/postgresql/template.sql", "r") as f:
        template = jinja2.Template(f.read())
    sql = template.render(params=params_mock.params)

    expected_sql = "DROPTABLEIFEXISTSviewflow.task_1_tmp;DROPTABLEIFEXISTSviewflow.task_1_old;CREATETABLEviewflow.task_1_tmpAS(SELECTuser_id,emailFROMmain_app.userslimit1);--Ifit'sthefirsttimeaviewiscreate,thefirstaltertableinthetransactionwillfailbecausethetabledoesn'texistCREATETABLEIFNOTEXISTSviewflow.task_1(LIKEviewflow.task_1_tmp);begintransaction;ALTERTABLEviewflow.task_1RENAMETOtask_1_old;ALTERTABLEviewflow.task_1_tmpRENAMETOtask_1;endtransaction;DROPTABLEviewflow.task_1_old;--Createaliases--CreatethetmpviewCREATEORREPLACEVIEWviewflow.task_1_aliasedAS(SELECT*FROMviewflow.task_1)WITHNOSCHEMABINDING;--Commentthetable--NOTE:Addmoremetadata:owner,tags,aliasCOMMENTONTABLEviewflow.task_1IS'ThisisaverysimplePostgrestaskwithanalias';"
    assert "".join(sql.split()) == expected_sql


def test_postgres_without_alias(monkeypatch):
    dag_1 = viewflow.create_dag("./tests/projects/postgresql/simple_dag")
    task_1 = dag_1.get_task("task_1")
    ti_1 = TaskInstance(task_1, datetime(2020, 1, 1))
    task_1.render_template_fields(ti_1.get_template_context())

    assert "Create aliases" not in task_1.sql

  
def test_postgres_adapter_comments():
    session = _create_postgres_session()

    dag = viewflow.create_dag("./tests/projects/postgresql/simple_dag_comments")
    task = dag.get_task("task_1")
    ti = TaskInstance(task, datetime(2020, 1, 1))

    with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS viewflow.task_1")

    ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True, session=session)

    with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn() as conn:
        with conn.cursor() as cur:
            sql_table_comments = """
            SELECT cols.table_name,  
                   cols.column_name, 
                   pg_catalog.col_description(c.oid, cols.ordinal_position::int)
              FROM pg_catalog.pg_class c, information_schema.columns cols
             WHERE cols.table_catalog = 'viewflow' AND cols.table_schema = 'viewflow'
               AND cols.table_name = 'task_1'
               AND cols.table_name = c.relname;
            """
            cur.execute(sql_table_comments)
            results = cur.fetchall()
            assert ("task_1", "user_id", "User ID") in results
            assert ("task_1", "email", "Email") in results

        with conn.cursor() as cur:
            sql_table_description = (
                "select obj_description('viewflow.task_1'::regclass);"
            )
            cur.execute(sql_table_description)
            results = cur.fetchall()
            assert ("Description",) in results


def test_sql_templating():
    dag = viewflow.create_dag("./tests/projects/postgresql/templated")

    task_1 = dag.get_task("task_1")
    ti_1 = TaskInstance(task_1, datetime(2020, 1, 1))
    task_1.render_template_fields(ti_1.get_template_context())

    assert "SELECT user_id, email FROM viewflow.users WHERE user_id=1" in task_1.sql
    assert "SELECT user_id, email FROM viewflow.users WHERE user_id=2" in task_1.sql
    assert "SELECT user_id, email FROM viewflow.users WHERE user_id=3" in task_1.sql
