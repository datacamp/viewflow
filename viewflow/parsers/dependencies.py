import re


def get_sql_dependencies(sql_query, schema_name, dag_name):
    """Given the name of a table and the text of its query"""
    # super simple SQL parsing: lowercase and without comments
    sql_query = sql_query.lower()
    # filter the SQL comments
    sql_query = re.sub("--.*\n", "", sql_query)
    sql_query = re.sub(re.compile(r"[\s]+", re.MULTILINE), " ", sql_query)

    view_matches = re.finditer(f"[^a-z\d_\.]({schema_name}\.[a-z\d_\.]*)", sql_query)
    views_used = [v for v in set(m.group(1) for m in view_matches)]
    dependencies_list = [{"task": i.split(".")[1], "dag": dag_name} for i in views_used]
    return dependencies_list


def get_python_dependencies(python_content, schema_name, dag_name):
    python_content = python_content.lower()
    python_content_lines = python_content.split("\n")
    views_used = []
    for line in python_content_lines:
        if "read_sql_table" in line and schema_name in line:
            view = line.split("(")[1].split(",")[0].replace('"', "")
            views_used.append(view)
    return [{"task": task, "dag": dag_name} for task in views_used]
