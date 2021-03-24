import pandas as pd

def top_3_user_xp(db_engine):
    """
    ---
    owner: viewflow-team
    description: Provide the top 3 users with the most XP.
    fields:
        user_id: The user id
        xp: The user amount of XP
    schema: viewflow_demo
    connection_id: postgres_demo
    ---
    """
    df = pd.read_sql_table("user_xp", db_engine, schema="viewflow_demo")
    return df.sort_values(by=["xp"], ascending=False).head(3)
    
    