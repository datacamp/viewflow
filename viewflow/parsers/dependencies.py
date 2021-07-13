import re


def get_sql_dependencies(sql_query, schema_name):
    """Given the name of a table and the text of its query"""
    # super simple SQL parsing: lowercase and without comments
    sql_query = sql_query.lower()
    # filter the SQL comments
    sql_query = re.sub("--.*\n", "", sql_query)
    sql_query = re.sub(re.compile(r"[\s]+", re.MULTILINE), " ", sql_query)

    view_matches = re.finditer(f"[^a-z\d_\.]{schema_name}\.([a-z\d_\.]*)", sql_query)
    dependencies_list = [v for v in set(m.group(1) for m in view_matches)]
    return dependencies_list


def get_python_dependencies(python_content, schema_name):
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
