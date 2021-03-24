import ast
import pathlib
import yaml

from typing import Any, Dict, Set
from viewflow.adapters.python.python_adapter import DependantDataFrame


class NoDocstringError(Exception):
    pass


class UnparseableDocstringError(Exception):
    pass


def get_dependencies_from_function_def(node: ast.FunctionDef) -> Set[str]:
    return set(
        arg.arg
        for arg in node.args.args
        if arg.annotation is not None
        and arg.annotation.id == DependantDataFrame.__name__
    )


def parse_yaml_in_docstring(docstring: str):
    raw_docstring_lines = docstring.split("\n")
    try:
        yaml_start = raw_docstring_lines.index("---")
        yaml_end = raw_docstring_lines.index("---", yaml_start + 1)
        yaml_header = "\n".join(raw_docstring_lines[(yaml_start + 1) : yaml_end])
        return yaml.safe_load(yaml_header)
    except (ValueError, yaml.scanner.ScannerError):
        raise UnparseableDocstringError()


def create_view_from_function_def(function_file: pathlib.Path, node: ast.FunctionDef):
    try:
        docstring = ast.get_docstring(node)
    except TypeError:
        raise NoDocstringError()
    if docstring is None:
        raise NoDocstringError()
    parsed_docstring = parse_yaml_in_docstring(docstring)
    implicit_dependencies = get_dependencies_from_function_def(node)
    explicit_dependencies = set(parsed_docstring.get("dependencies", []))
    dependencies = implicit_dependencies | explicit_dependencies
    return dict(
        **parsed_docstring,
        type="PythonToPostgresOperator",
        inject_dependencies=True,
        dependencies=dependencies,
        depends_on=list(dependencies),
        name=node.name,
        python_dir=str(function_file.parent),
        callable=f"{function_file.stem}.{node.name}",
        content=function_file.read_text(),
    )


class PythonViewParser(ast.NodeVisitor):
    file: pathlib.Path

    def __init__(self, file: pathlib.Path):
        self.views = []
        self.file = file

    def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
        try:
            self.views.append(create_view_from_function_def(self.file, node))
        except (NoDocstringError, UnparseableDocstringError):
            pass

        return super().generic_visit(node)


def parse_python(file: pathlib.Path) -> Dict[str, Any]:
    tree = ast.parse(file.read_text())
    python_view_parser = PythonViewParser(file)
    python_view_parser.visit(tree)
    if len(python_view_parser.views) > 0:
        return python_view_parser.views[0]
    else:
        return None
