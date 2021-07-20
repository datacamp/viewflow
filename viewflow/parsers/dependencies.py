import re
from typing import List
import dependencies_custom_pattern

def get_sql_dependencies(sql_query, schema_name) -> List[str]:
    """Given the name of a table and the text of its query"""
    # super simple SQL parsing: lowercase and without comments
    sql_query = sql_query.lower()
    # filter the SQL comments
    sql_query = re.sub("--.*\n", "", sql_query)
    sql_query = re.sub(re.compile(r"[\s]+", re.MULTILINE), " ", sql_query)

    view_matches = re.finditer(f"[^a-z\d_\.]{schema_name}\.([a-z\d_\.]*)", sql_query)
    dependencies_list = [v for v in set(m.group(1) for m in view_matches)]
    return dependencies_list


def get_python_dependencies(python_content, schema_name) -> List[str]:
    python_content = python_content.lower()
    python_content_lines = python_content.split("\n")
    views_used = []
    # Extract the 'table_name' argument from the 'read_sql_table' function
    # if it is called on the given schema_name
    for line in python_content_lines:
        if "read_sql_table" in line and schema_name in line:
            if re.search(r"table_name\s*=", line):
                match = re.search(r"\s*table_name\s*=\s*(.+?)\s*,", line)
                if not match: continue
                view = match.group(1)
            else:
                view = line.split("(")[1].split(",")[0]
            view = re.sub(r"['|\"|\s]", "", view)
            views_used.append(view)
    return list(set(views_used))


# TODO finish, verify, rmd_content or r_content?
def get_rmd_dependencies(rmd_content, schema_name, task_name, custom_function: str) -> List[str]:
    import dependencies_custom_pattern as dcp
    if custom_function:
        return getattr(dcp, custom_function)(rmd_content, schema_name, task_name)
    return getattr(dcp, "get_dependencies_default")(rmd_content, schema_name, task_name)
    

def get_r_dependencies(r_content, schema_name, custom_function: str) -> List[str]:
    if custom_function:
        return getattr(dependencies_custom_pattern, custom_function)(r_content, schema_name)
    return getattr(dependencies_custom_pattern, "get_dependencies_default")(r_content, schema_name)
