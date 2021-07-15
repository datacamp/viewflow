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


def get_rmd_dependencies(rmd_content, schema_name, task_name):
    """Get dependencies from Rmd file"""
    rmd_content = rmd_content.lower()
    rmd_content = re.sub(re.compile(r"\/\*.*\*\/", re.MULTILINE), "", rmd_content)
    rmd_content = re.sub("--.*\n", "", rmd_content)
    rmd_content = re.sub(re.compile(r"[\s]+", re.MULTILINE), " ", rmd_content)
    view_matches = re.finditer(
        f"[^a-z\d_\.](tbl_{schema_name}\_[a-z\d_\.]*)", rmd_content
    )
    views_used = [
        v.replace(f"tbl_{schema_name}_", "")
        for v in set(m.group(1) for m in view_matches)
        if v != task_name
    ]
    view_matches_2 = re.finditer(
        f"[^a-z\d_\.]{schema_name}\.([a-z\d_\.]*)", rmd_content
    )
    views_used_2 = [
        v for v in set(m.group(1) for m in view_matches_2) if v != task_name
    ]
    return list(set(views_used + views_used_2))