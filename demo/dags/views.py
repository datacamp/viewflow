from viewflow import create_dags

DAG = create_dags("./dags", globals(), "viewflow_demo")