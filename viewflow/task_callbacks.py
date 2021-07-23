

def success_callback(airflow_context):
    pass


def retry_callback(airflow_context):
    pass


def failure_callback(airflow_context):
    info = {
        "task": airflow_context.get('task_instance').task_id,
        "dag": airflow_context.get('task_instance').dag_id,
        "owner": airflow_context["task"].owner,
        "execution_time": airflow_context.get('execution_date'),
        "URL_logs": {airflow_context.get('task_instance').log_url}
    }
    # TODO send email

