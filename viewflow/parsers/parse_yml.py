import pathlib
import yaml
from typing import Any, Dict


def parse_yml(file: pathlib.Path) -> Dict[str, Any]:
    return yaml.safe_load(file.read_text())
