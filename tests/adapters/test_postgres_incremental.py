import viewflow
from datetime import datetime, date

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

    with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS viewflow.emails_blog")

    # Table 'emails_blog' does not yet exist --> query must be run with initial parameters
    ti = TaskInstance(task, datetime(2020, 1, 1))
    ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True, session=session)
    with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM viewflow.emails_blog")
            (count,) = cur.fetchone()
            assert count == 1
            
            cur.execute("SELECT * FROM viewflow.emails_blog")
            (user_id, notification_mode, email, updated_at, __view_generated_at) = cur.fetchone()
            assert user_id == 1
            assert notification_mode == "selection"
            assert email == "test1@datacamp.com"
            assert updated_at == datetime.strptime("2021-12-01 12:00:00", "%Y-%m-%d %H:%M:%S")
            assert __view_generated_at == date.today()
    
    # First incremental update --> additional rows are added (only 1 in this case)
    ti = TaskInstance(task, datetime(2020, 1, 1))
    ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True, session=session)
    with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM viewflow.emails_blog")
            (count,) = cur.fetchone()
            assert count == 2
    
    # Second incremental update --> additional row is added
    ti = TaskInstance(task, datetime(2020, 1, 1))
    ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True, session=session)
    with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM viewflow.emails_blog")
            (count,) = cur.fetchone()
            assert count == 3

    # User 1 disables the blog notifications
    with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE viewflow.notifications
                SET notification_mode='off', updated_at=timestamp '2024-9-01 12:00:00'
                WHERE user_id=1 AND category='blog'
            """)

    # Third incremental update --> changed row must be updated
    ti = TaskInstance(task, datetime(2020, 1, 1))
    ti.run(ignore_task_deps=True, ignore_ti_state=True, test_mode=True, session=session)
    with PostgresHook(postgres_conn_id="postgres_viewflow").get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM viewflow.emails_blog")
            (count,) = cur.fetchone()
            assert count == 3

            cur.execute("SELECT notification_mode, updated_at FROM viewflow.emails_blog WHERE user_id = 1")
            (notification_mode, updated_at) = cur.fetchone()

            assert notification_mode == "off"
            assert updated_at == datetime.strptime("2024-9-01 12:00:00", "%Y-%m-%d %H:%M:%S")

            # Restore change to user 1's notification mode
            cur.execute("""
                UPDATE viewflow.notifications
                SET notification_mode='selection', updated_at=timestamp '2021-12-01 12:00:00'
                WHERE user_id=1 AND category='blog';
            """)
