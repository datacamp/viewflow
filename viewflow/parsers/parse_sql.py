import pathlib
import yaml
import re
from typing import Any, Dict


def parse_sql(file: pathlib.Path) -> Dict[str, Any]:
    content = file.read_text().split("\n")
    l = [i for i, x in enumerate(content) if re.search("---", x)]
    yml = "\n".join(content[l[0] + 1 : l[1]])
    task_config = yaml.safe_load(yml)
    extras = {
        "type": task_config.get("type", "PostgresOperator"),
        "content": "\n".join(content[0 : max(l[0]-1,0)] + content[(l[1] + 2) :]),
        "task_file_path": str(file),
    }
    task_config.update(extras)
    return task_config
