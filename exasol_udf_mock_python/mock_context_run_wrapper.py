from exasol_udf_mock_python.mock_context import MockContext


def _disallowed_function(*args, **kw):
    raise RuntimeError(
        "F-UDF-CL-SL-PYTHON-1107: next(), reset() and emit() "
        "functions are not allowed in scalar context")


class MockContextRunWrapper:

    def __init__(self, mock_context: MockContext, input_type: str,
                 output_type: str, is_variadic_input: bool):
        self._output_type = output_type
        self._input_type = input_type
        self._mock_context = mock_context
        self._is_variadic_input = is_variadic_input
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
        """
        Variadic UDFs' columns are only integer values. Since integers are not
        valid identifier in python, this method cannot be used by variadic UDFs.
        """
        if self._is_variadic_input:
            raise RuntimeError(f"E-UDF-CL-SL-PYTHON-1085: Iterator has no "
                               f"object with name '{name}'")
        return self._mock_context.__getattr__(name)

    def __getitem__(self, item):
        """
        Variadic UDFs can retrieve items by index. The index value can be given
        as an integer (e.g. ctx[1]) or as a string integer (e.g. ctx["1"]).

        Non-variadic UDFs can retrieve items by index, that can be either an
        integer (e.g. ctx[1]) or a column name (e.g. ctx["col_name"]). They do
        not accept string integers as index.
        """
        item = int(item) if self._is_variadic_input else item
        if isinstance(item, int):
            return self._mock_context._data[item]
        else:
            try:
                return self._mock_context.__getattr__(item)
            except KeyError:
                raise RuntimeError(f"E-UDF-CL-SL-PYTHON-1082: Column with name "
                                   f"'{item}' does not exist")
