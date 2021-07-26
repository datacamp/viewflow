import viewflow
from unittest.mock import patch, ANY, call


@patch("viewflow.parsers.dependencies_r_patterns.custom_get_dependencies")
@patch("viewflow.parsers.dependencies_r_patterns.get_dependencies_default")
def test_default_dependencies_pattern(get_default_mock, get_custom_mock):

    viewflow.create_dag("./tests/projects/r/pattern_default")

    # Dependencies must have been retrieved for all possible schema's
    calls = [call(ANY, "viewflow"), call(ANY, "public")]
    get_default_mock.assert_has_calls(calls, any_order=True)
    get_custom_mock.assert_not_called()



@patch("viewflow.parsers.dependencies_r_patterns.custom_get_dependencies")
@patch("viewflow.parsers.dependencies_r_patterns.get_dependencies_default")
def test_custom_dependencies_pattern(get_default_mock, get_custom_mock):

    viewflow.create_dag("./tests/projects/r/pattern_custom")

    # Dependencies must have been retrieved for all possible schema's
    get_default_mock.assert_not_called()
    calls = [call(ANY, "viewflow"), call(ANY, "public")]
    get_custom_mock.assert_has_calls(calls, any_order=True)
