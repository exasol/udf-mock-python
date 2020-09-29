import pytest
from exasol_udf_mock_python.mock_test_executor import *

def test_different_udf_wrapper_function_names():
    def udf_wrapper():
        
        def run(ctx):
            pass

    def udf_wrapper2():
        
        def run(ctx):
            pass

    def my_wrapper():
        
        def run(ctx):
            pass


    executor = MockTestExecutor()
    meta = MockMetaData(
            script_code_wrapper_function=udf_wrapper,
            input_type="SET",
            input_columns=[Column("t", int, "INTEGER")],
            output_type="EMITS",
            output_columns=[Column("t", int, "INTEGER")]
            )
    exa = MockExaEnvironment(meta)
    result=executor.run([], exa)
    assert result == []

    meta = MockMetaData(
            script_code_wrapper_function=udf_wrapper2,
            input_type="SET",
            input_columns=[Column("t", int, "INTEGER")],
            output_type="EMITS",
            output_columns=[Column("t", int, "INTEGER")]
            )
    exa = MockExaEnvironment(meta)
    result=executor.run([], exa)
    assert result == []

    meta = MockMetaData(
            script_code_wrapper_function=my_wrapper,
            input_type="SET",
            input_columns=[Column("t", int, "INTEGER")],
            output_type="EMITS",
            output_columns=[Column("t", int, "INTEGER")]
            )
    exa = MockExaEnvironment(meta)
    result=executor.run([], exa)
    assert result == []

def test_udf_wrapper_with_docstring_on_next_line():
    def udf_wrapper():
        """
        wrapper with docstring should raise Exception, 
        because their is no easy way to remove docstrings 
        to get only the source witin the function
        """

        def run(ctx):
            pass

    executor = MockTestExecutor()
    meta = MockMetaData(
            script_code_wrapper_function=udf_wrapper,
            input_type="SET",
            input_columns=[Column("t", int, "INTEGER")],
            output_type="EMITS",
            output_columns=[Column("t", int, "INTEGER")]
            )
    exa = MockExaEnvironment(meta)
    result=executor.run([], exa)
    assert result == []

def test_udf_wrapper_with_docstring_after_empty_lines():
    def udf_wrapper():


        """
        wrapper with docstring should raise Exception, 
        because their is no easy way to remove docstrings 
        to get only the source witin the function
        """

        def run(ctx):
            pass

    executor = MockTestExecutor()
    meta = MockMetaData(
            script_code_wrapper_function=udf_wrapper,
            input_type="SET",
            input_columns=[Column("t", int, "INTEGER")],
            output_type="EMITS",
            output_columns=[Column("t", int, "INTEGER")]
            )
    exa = MockExaEnvironment(meta)
    result=executor.run([], exa)
    assert result == []

def test_udf_wrapper_with_no_empty_line_after_function_name():
    def udf_wrapper():
        def run(ctx):
            pass

    executor = MockTestExecutor()
    meta = MockMetaData(
            script_code_wrapper_function=udf_wrapper,
            input_type="SET",
            input_columns=[Column("t", int, "INTEGER")],
            output_type="EMITS",
            output_columns=[Column("t", int, "INTEGER")]
            )
    exa = MockExaEnvironment(meta)
    result=executor.run([], exa)
    assert result == []

def test_udf_wrapper_with_white_spaces_in_function_definition():
    def   udf_wrapper  (  )  :  

        def run(ctx):
            pass

    executor = MockTestExecutor()
    meta = MockMetaData(
            script_code_wrapper_function=udf_wrapper,
            input_type="SET",
            input_columns=[Column("t", int, "INTEGER")],
            output_type="EMITS",
            output_columns=[Column("t", int, "INTEGER")]
            )
    exa = MockExaEnvironment(meta)
    result=executor.run([], exa)
    assert result == []

def test_exception_udf_wrapper_with_parameter():
    def udf_wrapper(param):
        def run(ctx):
            pass

    executor = MockTestExecutor()
    with pytest.raises(Exception):
        meta = MockMetaData(
                script_code_wrapper_function=udf_wrapper,
                input_type="SET",
                input_columns=[Column("t", int, "INTEGER")],
                output_type="EMITS",
                output_columns=[Column("t", int, "INTEGER")]
                )
        exa = MockExaEnvironment(meta)
        result=executor.run([], exa)

