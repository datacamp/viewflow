import viewflow
from datetime import datetime
import re

from airflow.models import TaskInstance, Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
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


def test_incremental_updates():
    session = _create_postgres_session()

    dag = viewflow.create_dag("./tests/projects/postgresql/incremental_operator")
    task = dag.get_task("emails_blog")
    ti = TaskInstance(task, datetime(2020, 1, 1))

    with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn().cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS viewflow.emails_blog")

    # Table 'emails_blog' does not yet exist --> query must be run with initial parameters
    ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True, session=session)
    # with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn().cursor() as cur:
    #     cur.execute("SELECT COUNT(*) FROM viewflow.emails_blog")
    #     (count,) = cur.fetchone()
    #     assert count == 1
        # cur.execute("SELECT * FROM viewflow.emails_blog")
        # (user_id, notification_mode, email, updated_at, __view_generated_at) = cur.fetchone()
        # assert user_id == 1 and notification_mode == "selection" \
        #     and email == "test1@datacamp.com" and updated_at == "2021-12-01 12:00:00" \
        #     and re.search(r"\d\{4}-\d\d-\d\d", __view_generated_at)
    
    # # First incremental update --> additional rows are added (only 1 in this case)
    # ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True, session=session)
    # with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn().cursor() as cur:
    #     cur.execute("SELECT COUNT(*) FROM viewflow.emails_blog")
    #     (count,) = cur.fetchone()
    #     assert count == 2
    
    # # Second incremental update --> additional row is added
    # ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True, session=session)
    # with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn().cursor() as cur:
    #     cur.execute("SELECT COUNT(*) FROM viewflow.emails_blog")
    #     (count,) = cur.fetchone()
    #     assert count == 3

    # # User 1 disables the blog notifications
    # with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn().cursor() as cur:
    #     cur.execute("""
    #         UPDATE viewflow.notifications
    #         SET notification_mode='off', updated_at='2024-9-01 12:00:00'
    #         WHERE user_id=1 AND category='blog';
    #     """)

    # # Third incremental update --> changed row must be updated
    # ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True, session=session)
    # with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn().cursor() as cur:
    #     cur.execute("SELECT COUNT(*) FROM viewflow.emails_blog")
    #     (count,) = cur.fetchone()
    #     assert count == 3

    #     cur.execute("SELECT notification_mode, updated_at FROM viewflow.emails_blog WHERE user_id = 1")
    #     (notification_mode, updated_at) = cur.fetchone()
    #     assert notification_mode == "off" and updated_at == "2024-9-01 12:00:00"

    #     # Restore change to user 1's notification mode
    #     cur.execute("""
    #         UPDATE viewflow.notifications
    #         SET notification_mode='selection', updated_at='2021-12-01 12:00:00'
    #         WHERE user_id=1 AND category='blog';
    #     """)
