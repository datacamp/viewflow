from viewflow.create_dag import create_dag
from viewflow.task_callbacks import on_success_callback_default, on_retry_callback_default, on_failure_callback_default, on_failure_callback_email

def test_default_callbacks():
    dag = create_dag("./tests/projects/postgresql/simple_dag")
    task = dag.get_task("task_1")

    assert task.on_success_callback == on_success_callback_default
    assert task.on_retry_callback == on_retry_callback_default
    assert task.on_failure_callback == on_failure_callback_default


def test_custom_callbacks():
    """No DAG defaults, but task has specified callbacks."""
    dag = create_dag("./tests/projects/r/pattern_default")
    task = dag.get_task("user_emails")

    assert task.on_success_callback == on_failure_callback_email
    assert task.on_retry_callback == on_failure_callback_email
    assert task.on_failure_callback == on_failure_callback_email


def test_dag_defaults_callbacks():
    """DAG has default callbacks, but these can be overwritten by individual tasks."""
    dag = create_dag("./tests/projects/dag_callbacks") # The DAG has specified default callbacks
    task1 = dag.get_task("task_1")
    task2 = dag.get_task("task_2")

    # Task 1 must have the DAG's default callbacks
    assert task1.on_success_callback == on_failure_callback_email
    assert task1.on_retry_callback == on_failure_callback_email
    assert task1.on_failure_callback == on_failure_callback_email

    # Task 2 must have the task's specific callbacks
    assert task2.on_failure_callback == on_success_callback_default
    assert task2.on_success_callback == on_retry_callback_default
    assert task2.on_retry_callback == on_failure_callback_default
