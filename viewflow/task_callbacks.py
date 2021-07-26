from datetime import datetime
from airflow.utils.email import send_email


def success_callback_default(airflow_context):
    pass


def retry_callback_default(airflow_context):
    pass


def failure_callback_default(airflow_context):
    pass


def failure_callback_email(airflow_context):
    """Send an email to the owner of the failed task.
    Note that you first have to configure the email settings in $AIRFLOW_HOME/airflow.cfg"""
    info = {
        "task": airflow_context.get("task_instance").task_id,
        "dag": airflow_context.get("task_instance").dag_id,
        "owner": airflow_context["task"].owner,
        "execution_time": airflow_context.get("execution_date"),
        "URL_logs": {airflow_context.get("task_instance").log_url}
    }
    
    recipient_emails=[airflow_context["task"].owner]

    title = ''
    body = ''

    send_email(recipient_emails, title, body)

