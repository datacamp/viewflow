import pathlib
from typing import Any, Dict
from frontmatter import Frontmatter


def parse_rmd(file: pathlib.Path) -> Dict[str, Any]:
    """Parse an Rmd File"""
    file_parsed = Frontmatter.read_file(file)
    task_config = file_parsed.get("attributes", {})
    extras = {
        "type": "RmdOperator",
        "content": file.read_text(),
        "task_file_path": str(file),
    }
    task_config.update(extras)
    return task_config