from exasol_udf_mock_python.mock_context import MockContext


def _disallowed_function(*args, **kw):
    raise RuntimeError(
        "F-UDF-CL-SL-PYTHON-1107: next(), reset() and emit() "
        "functions are not allowed in scalar context")


class MockContextRunWrapper:

    def __init__(self, mock_context: MockContext, input_type: str,
                 output_type: str, is_variadic: bool):
        self._output_type = output_type
        self._input_type = input_type
        self._mock_context = mock_context
        self._is_variadic = is_variadic
        if self._output_type == "RETURNS":
            self.emit = _disallowed_function
        else:
            self.emit = self._mock_context.emit
        if self._input_type == "SCALAR":
            self.next = _disallowed_function
            self.reset = _disallowed_function
        else:
            self.next = self._mock_context.next
            self.reset = self._mock_context.reset
            self.get_dataframe = self._mock_context.get_dataframe
            self.size = self._mock_context.size

    def __getattr__(self, name):
        if self._is_variadic:
            raise RuntimeError(f"E-UDF-CL-SL-PYTHON-1085: Iterator has no "
                               f"object with name '{name}'")
        return self._mock_context.__getattr__(name)

    def __getitem__(self, item):
        return self._mock_context._data[item]
