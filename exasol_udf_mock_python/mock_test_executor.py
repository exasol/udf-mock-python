import itertools
from enum import Enum
from multiprocessing import Lock
from multiprocessing.pool import Pool
from typing import Dict, List, Any, Tuple
import dill
import pandas as pd
import textwrap
import re

class Column:
    def __init__(self, name, type, sql_type, precision=None, scale=None, length=None):
        self.name = name
        self.type = type
        self.sql_type = sql_type
        self.precision = precision
        self.scale = scale
        self.length = length

    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__)


class ConnectionType(Enum):
    PASSWORD = 1


class Connection:
    def __init__(self, address: str, user: str = None, password: str = None,
                 type: ConnectionType = ConnectionType.PASSWORD):
        self.type = type
        self.password = password
        self.user = user
        self.address = address

    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__)


class MockMetaData:
    def __init__(
            self,
            script_code_wrapper_function,
            input_columns: List[Column], 
            input_type: str,
            output_columns: List[Column], 
            output_type: str,
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
        self._script_language = "PYTHON3"
        self._script_name = script_name
        self._script_schema = script_schema
        self._current_user = current_user
        self._current_schema = current_schema
        self._scope_user = scope_user
        function_code = textwrap.dedent(dill.source.getsource(script_code_wrapper_function))
        function_name = script_code_wrapper_function.__name__
        starts_with_pattern = r"^def[ \t]+"+function_name+r"[ \t]*\([ \t]*\)[ \t]*:[ \t]*\n"
        print("starts_with_pattern",starts_with_pattern)
        match = re.match(starts_with_pattern,function_code)
        print("match", match)
        if match is not None:
            self._script_code = textwrap.dedent("\n".join(function_code.split("\n")[1:]))
        else:
            formatted_starts_with_pattern = starts_with_pattern.replace("\n","\\n").replace("\t","\\t")
            raise Exception((
                textwrap.dedent(
                    f"""
                    The script_code_wrapper_function has the wrong header.
                    It needs to start with \"{formatted_starts_with_pattern}\". 
                    However, we got:
                    
                    """)+
                    function_code).strip())
            
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
        self._input_column_count = len(input_columns)
        self._input_columns = input_columns
        self._output_type = output_type
        self._output_column_count = len(output_columns)
        self._output_columns = output_columns

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
    def input_columns_count(self):
        return self._input_column_count

    @property
    def input_columns(self):
        return self._input_columns

    @property
    def output_type(self):
        return self._output_type

    @property
    def output_columns_count(self):
        return self._output_column_count

    @property
    def output_columns(self):
        return self._output_columns

    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

class MockExaEnvironment:
    def __init__(self, 
            metadata: MockMetaData, 
            connections: Dict[str, Connection]=None
            ):
        self._connections = connections
        if self._connections is None:
            self._connections = {}
        self._metadata = metadata

    def get_connection(self, name:str)->Connection:
        return self._connections[name]

    def import_script(self, schema_script_name:str):
        raise Exception("Import Script is currently not supported by this mock. We also advise against its usage in general and use instead proper packaging mechanism of your the language in choice.")

    @property
    def meta(self):
        return self._metadata

    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

class MockContext:

    def __init__(self, inputs:List[Tuple[Any]], metadata:MockMetaData):
        self._inputs = inputs                               # actual data
        self._outputs = []
        self._iter = iter(self._inputs)
        self.next()
        self._metadata = metadata
        self._name_position_map = \
            {column.name: position
             for position, column
             in enumerate(metadata.input_columns)}

    def get_dataframe(self, num_rows='all', start_col=0):
        if self._data is None:
            return None
        if num_rows == 'all':
            iter = self._iter
        else:
            iter = itertools.islice(self._iter, num_rows - 1)
        columns_ = [column.name for column in self._metadata.input_columns]
        df_next = pd.DataFrame.from_records(data=iter, columns=columns_)
        df_current = pd.DataFrame.from_records([self._data], columns=columns_)
        df = df_current.append(df_next)
        df = df.reset_index(drop=True)
        if df.empty:
            return None
        else:
            self.next()
            return df

    @property
    def data(self):
        return self._data

    def __getattr__(self, name):
        return self._data[self._name_position_map[name]]

    def next(self):
        try:
            new_data = next(self._iter)
            self._data = new_data
            return True
        except StopIteration as e:
            self._data = None
            return False

    def size(self):
        return len(self._inputs)

    def reset(self):
        self._iter = iter(self._inputs)
        self.next()

    def emit(self, *args):
        self._outputs.append(args)
        return


class MockTestExecutor:
    _lock = Lock()

    def _exec_run(self, exec_globals: Dict[str,Any], ctx: MockContext):
        codeObject = compile("run(__mock_test_executor_ctx)", 'exec_run', 'exec')
        exec_locals={}
        exec_globals["__mock_test_executor_ctx"]=ctx
        exec(codeObject, exec_globals, exec_locals)
        
    def _exec_cleanup(self, exec_globals: Dict[str,Any]):
        codeObject = compile("cleanup()", 'exec_cleanup', 'exec')
        exec(codeObject, exec_globals)

    def _exec_init(self, exa_environment:MockExaEnvironment) -> Dict[str,Any]:
        codeObject = compile(exa_environment.meta.script_code, 'udf', 'exec')
        exec_globals = {"exa":exa_environment}
        exec(codeObject, exec_globals)
        return exec_globals

    def run(self, inputs, exa_environment:MockExaEnvironment):
        with self._lock:
            ctx = MockContext(inputs, exa_environment.meta)
            exec_globals=self._exec_init(exa_environment)
            try:
                self._exec_run(exec_globals,ctx)
            finally:
                if "cleanup" in exec_globals:
                    self._exec_cleanup(_exec_cleanup)
            return ctx._outputs
