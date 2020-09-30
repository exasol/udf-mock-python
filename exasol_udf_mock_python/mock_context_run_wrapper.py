from exasol_udf_mock_python.mock_context import MockContext


def _disallowed_function(*args, **kw):
    raise RuntimeError(
        "F-UDF-CL-SL-PYTHON-1107: next(), reset() and emit() functions are not allowed in scalar context")

class MockContextRunWrapper:

    def __init__(self, mock_context: MockContext, input_type: str, output_type: str):
        self._output_type = output_type
        self._input_type = input_type
        self._mock_context = mock_context
        if self._output_type == "RETURNS":
            self.emit = _disallowed_function
        else:
            self.emit = self._mock_context.emit
        if self._output_type == "SCALAR":
            self.next = _disallowed_function
            self.reset = _disallowed_function
        else:
            self.next = self._mock_context.next
            self.reset = self._mock_context.reset

    def get_dataframe(self, num_rows='all', start_col=0):
        return self._mock_context.get_dataframe(num_rows, start_col)

    def __getattr__(self, name):
        return self._mock_context.__getattr__(name)

    def size(self):
        return self._mock_context.size()