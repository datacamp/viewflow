import pathlib
from typing import Any, Dict
import re
import yaml

def parse_rmd(file: pathlib.Path) -> Dict[str, Any]:
    """Parse an Rmd File"""
    content = file.read_text().split("\n")
    l = [i for i, x in enumerate(content) if re.search(r"^---", x)]
    temp = "\n".join(content[l[0] + 1 : l[1]])
    yml = re.sub(r"# ", "", temp)
    task_config = yaml.safe_load(yml)
    extras = {
        "type": "RmdOperator",
        "content": "\n".join(content[0 : l[0]] + content[(l[1] + 1) :]),
        "task_file_path": str(file)
    }
    task_config.update(extras)
    return task_config
