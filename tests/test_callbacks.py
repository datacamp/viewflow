from viewflow.create_dag import create_dag
from viewflow.task_callbacks import success_callback_default, retry_callback_default, failure_callback_default, failure_callback_email

def test_default_callbacks():
    dag = create_dag("./tests/projects/postgresql/simple_dag")
    task = dag.get_task("task_1")

    assert task.on_success_callback == success_callback_default
    assert task.on_retry_callback == retry_callback_default
    assert task.on_failure_callback == failure_callback_default


def test_custom_callbacks():
    dag = create_dag("./tests/projects/r/pattern_default")
    task = dag.get_task("user_emails")

    assert task.on_success_callback == failure_callback_email
    assert task.on_retry_callback == failure_callback_email
    assert task.on_failure_callback == failure_callback_email
