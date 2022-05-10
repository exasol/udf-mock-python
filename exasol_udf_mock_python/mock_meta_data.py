import re
import dill
import textwrap
from typing import List
from exasol_udf_mock_python.column import Column


class MockMetaData:
    def __init__(
            self,
            script_code_wrapper_function,
            input_columns: List[Column],
            input_type: str,
            output_columns: List[Column],
            output_type: str,
            is_variadic: bool = False,
            script_name: str="TEST_UDF",
            script_schema: str="TEST_SCHEMA",
            current_user: str="sys",
            current_schema: str="TEST_SCHEMA",
            scope_user: str="sys",
            connection_id: str="123123",
            database_name: str="TEST_DB",
            database_version: str="7.0.0",
            node_count: int="1",
            node_id: int="0",
            vm_id: int="123",
            session_id: int="123456789",
            statement_id: int="123456789",
            memory_limit: int=4*1073741824,
            ):

        assert input_type.upper() in ["SET", "SCALAR"]
        assert output_type.upper() in ["EMITS", "RETURNS"]
        if is_variadic:
            for i in range(len(input_columns)):
                assert str(i+1) == str(input_columns[i].name)

        self._script_language = "PYTHON3"
        self._script_name = script_name
        self._script_schema = script_schema
        self._current_user = current_user
        self._current_schema = current_schema
        self._scope_user = scope_user
        self._script_code = self._extract_script_code(script_code_wrapper_function)
        self._connection_id = connection_id
        self._database_name = database_name
        self._database_version = database_version
        self._node_count = node_count
        self._node_id = node_id
        self._vm_id = vm_id
        self._session_id = session_id
        self._statement_id = statement_id
        self._memory_limit = memory_limit
        self._input_type = input_type
        self._validate_column_defintions(input_columns)
        self._input_column_count = len(input_columns)
        self._input_columns = input_columns
        self._output_type = output_type
        self._validate_column_defintions(output_columns)
        self._output_column_count = len(output_columns)
        self._output_columns = output_columns
        self._is_variadic = is_variadic

    def _extract_script_code(self, script_code_wrapper_function):
        function_code = textwrap.dedent(dill.source.getsource(script_code_wrapper_function))
        function_name = script_code_wrapper_function.__name__
        starts_with_pattern = r"^def[ \t]+" + function_name + r"[ \t]*\([ \t]*\)[ \t]*:[ \t]*\n"
        match = re.match(starts_with_pattern, function_code)
        if match is not None:
            return textwrap.dedent("\n".join(function_code.split("\n")[1:]))
        else:
            formatted_starts_with_pattern = starts_with_pattern.replace("\n", "\\n").replace("\t", "\\t")
            raise Exception((
                                    textwrap.dedent(
                                        f"""
                    The script_code_wrapper_function has the wrong header.
                    It needs to start with \"{formatted_starts_with_pattern}\". 
                    However, we got:
                    
                    """) +
                                    function_code).strip())

    def _validate_column_defintions(self, columns):
        column_names_set = {column.name for column in columns}
        if len(column_names_set) != len(columns):
            column_names_list = sorted([column.name for column in columns])
            raise TypeError(f"Found duplicate column names in {column_names_list}")

    def convert_column_description(self, input_columns):
        return [(column.name, column.type, column.sql_type,
                 column.precision, column.scale, column.length)
                for column in input_columns]

    @property
    def script_language(self):
        return self._script_language

    @property
    def script_name(self):
        return self._script_name

    @property
    def script_schema(self):
        return self._script_schema

    @property
    def current_user(self):
        return self._current_user

    @property
    def current_schema(self):
        return self._current_schema

    @property
    def scope_user(self):
        return self._scope_user

    @property
    def script_code(self):
        return self._script_code

    @property
    def connection_id(self):
        return self._connection_id

    @property
    def database_name(self):
        return self._database_name

    @property
    def database_version(self):
        return self._database_version

    @property
    def node_count(self):
        return self._node_count

    @property
    def node_id(self):
        return self._node_id

    @property
    def vm_id(self):
        return self._vm_id

    @property
    def session_id(self):
        return self._session_id

    @property
    def statement_id(self):
        return self._statement_id

    @property
    def memory_limit(self):
        return self._memory_limit

    @property
    def input_type(self):
        return self._input_type

    @property
    def input_column_count(self):
        return self._input_column_count

    @property
    def input_columns(self):
        return self._input_columns

    @property
    def output_type(self):
        return self._output_type

    @property
    def output_column_count(self):
        return self._output_column_count

    @property
    def output_columns(self):
        return self._output_columns

    @property
    def is_variadic(self):
        return self._is_variadic

    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__)
