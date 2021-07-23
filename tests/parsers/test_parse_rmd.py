from pathlib import Path
from viewflow.parsers.parse_rmd import parse_rmd


def test_parse_rmd():
    sql_file = Path("./tests/projects/rmd/task_1.Rmd")
    task_parsed = parse_rmd(sql_file)
    expected = {
        'connection_id': 'postgres_demo',
        'content': '\n## Title 1\nSection 1\n```{r rename_view}\nuser_xp <- viewflow_demo.user_xp\n```\n\n## Title 2\nSection 2\n```{r create_view}\ntop_3_user_xp_duplicate <- head(user_xp[order(user_xp$xp, decreasing=TRUE),], n = 3)\n```\n',
        'description': 'Provide the top 3 users with the most XP.',
        'fields': {'user_id': 'The user id', 'xp': 'The user amount of XP'},
        'owner': 'viewflow-team',
        'schema': 'viewflow_demo',
        'task_file_path': 'tests/projects/rmd/task_1.Rmd',
        'type': 'RmdOperator'
    }
    assert task_parsed == expected
