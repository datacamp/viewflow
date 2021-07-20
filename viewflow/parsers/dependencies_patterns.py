import re
from typing import List, Dict

def get_dependencies_default(r_content, schema_name) -> Dict[str, str]:
    """Return a dictionary (script_name -> table_name) for tables under the given schema_name that are used in r_content.
    By default, the tables used in the R script must be referred to in the <schema>.<table> format.
    E.g. if 'viewflow_raw.users' is mentioned in r_content, then ('viewflow_raw.users' -> 'users') will be added to the dictionary."""
    view_matches = re.finditer(f"[^a-z\d_\.]({schema_name}\.([a-z\d_\.]*))", r_content)
    res = dict()
    for m in view_matches:
        res[m.group(1)] = m.group(2)
    return res


def custom_get_dependencies(r_content, schema_name) -> Dict[str, str]:
    """Write a custom function to extract the tables from r_content, the code should be similar to get_dependencies_default.
    This method is activated by adding a new line to the metadata in the R script: 'dependency_function: custom_get_dependencies'
    A dictionary must be returned which maps the table name that is used in r_content to the table under the given schema.
    E.g. if you choose to refer to the 'users' table in the 'viewflow_raw' schema as 'table.viewflow_raw.users',
    then the dictionary must contain ('table.viewflow_raw.users' -> 'users')"""
    return dict()



# TODO return dict?
def custom_get_rmd_dependencies(rmd_content, schema_name, task_name) -> List[str]:
    """Get dependencies from Rmd file, custom patterns used by DataCamp.
    The patterns correspond to custom method calls and SQL queries in the Rmd script"""
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
