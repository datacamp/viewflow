import pandas as pd

from viewflow.adapters.python.python_adapter import DependantDataFrame


def email_domains(emails: DependantDataFrame, db_engine=None):
    """
    ---
    owner: data@datacamp.com
    description: This is a very simple python to Postgres task.
    fields:
        a: Column a
        b: Colum b
    schema: viewflow
    connection_id: postgres_viewflow
    ---
    """
    if isinstance(emails, pd.DataFrame):
        return (
            emails.email.str.split("@", expand=True)
            .rename(columns={1: "domain"})[["domain"]]
            .drop_duplicates()
        )
    else:
        return pd.DataFrame()
