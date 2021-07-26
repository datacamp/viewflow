from datetime import datetime
from airflow.utils.email import send_email
from textwrap import dedent

def success_callback_default(airflow_context):
    pass


def retry_callback_default(airflow_context):
    pass


def failure_callback_default(airflow_context):
    pass


def failure_callback_email(airflow_context):
    """Send an email to the owner of the failed task.
    Note that you first have to configure the email (SMTP) settings in $AIRFLOW_HOME/airflow.cfg"""

    title = f"""Airflow failed task: {airflow_context.get("task_instance").task_id}"""
    body = dedent(f"""
    DAG:            {airflow_context.get("task_instance").dag_id}
    Task:           {airflow_context.get("task_instance").task_id}
    Owner:          {airflow_context.get("task").owner}
    Execution time: {airflow_context.get("execution_date")}
    URL logs:       {airflow_context.get("task_instance").log_url}
    """)

    recipient_emails = [airflow_context["task"].owner]
    send_email(recipient_emails, title, body)
