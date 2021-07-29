from pathlib import Path
from viewflow.parsers.parse_r import parse_r
from textwrap import dedent

def test_parse_r():
    sql_file = Path("./tests/projects/r/task_1.R")
    task_parsed = parse_r(sql_file)
    expected = {
        'connection_id': 'postgres_demo',
        'description': 'Provide the total amount of XP for each user',
        'fields': {'user_id': 'The user id', 'xp': 'The sum of XP'},
        'owner': 'viewflow-team',
        'schema': 'viewflow_demo',
        'task_file_path': 'tests/projects/r/task_1.R',
        'type': 'ROperator',
        'content': dedent("""
            library(dplyr)

            temp <- select(merge(viewflow_raw.users, viewflow_raw.user_course, by.x='id', by.y='user_id', all.x=TRUE), c('id', 'course_id'))
            names(temp)[names(temp) == 'id'] <- 'user_id'
            user_all_xp <- select(merge(temp, viewflow_raw.courses, by.x='course_id', by.y='id'), c('user_id', 'xp'))

            user_xp_duplicate <- aggregate(xp ~ user_id, user_all_xp, sum)[order(user_id)]
            """)
    }
    assert task_parsed == expected
