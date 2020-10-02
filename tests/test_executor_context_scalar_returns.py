import pytest

from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.udf_mock_executor import UDFMockExecutor


def test_emit_not_allowed():
    def udf_wrapper():

        def run(ctx):
            ctx.emit(ctx.t)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SCALAR",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="RETURNS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    with pytest.raises(RuntimeError):
        result = executor.run([Group([(1,), (5,), (6,)])], exa)

def test_next_not_allowed():
    def udf_wrapper():

        def run(ctx):
            ctx.next()

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SCALAR",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="RETURNS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    with pytest.raises(RuntimeError):
        result = executor.run([Group([(1,), (5,), (6,)])], exa)

def test_reset_not_allowed():
    def udf_wrapper():

        def run(ctx):
            ctx.reset()

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SCALAR",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="RETURNS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    with pytest.raises(RuntimeError):
        result = executor.run([Group([(1,), (5,), (6,)])], exa)

def test_simple_return():
    def udf_wrapper():

        def run(ctx):
            return ctx.t+1

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SCALAR",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="RETURNS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,), (5,), (6,)])], exa)
    assert result == [Group([(2,), (6,), (7,)])]

def test_multi_column_type():
    def udf_wrapper():

        def run(ctx):
            return ctx.t1+1, ctx.t2+1.1, ctx.t3+"1"

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SCALAR",
        input_columns=[Column("t1", int, "INTEGER"),
                       Column("t2", float, "FLOAT"),
                       Column("t3", str, "VARCHAR(20000)")],
        output_type="RETURNS",
        output_columns=[Column("t1", int, "INTEGER"),
                        Column("t2", float, "FLOAT"),
                        Column("t3", str, "VARCHAR(20000)")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,1.0,"1"), (5,5.0,"5"), (6,6.0,"6")])], exa)
    assert result == [Group([(2,2.1,"11"), (6,6.1,"51"), (7,7.1,"61")])]

def test_return_single_column_none():
    def udf_wrapper():

        def run(ctx):
            return None

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SCALAR",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="RETURNS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,), (5,), (6,)])], exa)
    assert result == [Group([(None,),(None,),(None,)])]

def test_return_multi_column_none():
    def udf_wrapper():

        def run(ctx):
            return None,None

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SCALAR",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="RETURNS",
        output_columns=[Column("t1", int, "INTEGER"),
                        Column("t2", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,), (5,), (6,)])], exa)
    assert result == [Group([(None,None),(None,None),(None,None)])]

def test_input_single_column_none():
    def udf_wrapper():

        def run(ctx):
            return ctx.t

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SCALAR",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="RETURNS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(None,), (5,), (6,)])], exa)
    assert result == [Group([(None,),(5,),(6,)])]

def test_input_multi_column_none():
    def udf_wrapper():

        def run(ctx):
            return ctx.t1,ctx.t2

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SCALAR",
        input_columns=[Column("t1", int, "INTEGER"),
                        Column("t2", int, "INTEGER")],
        output_type="RETURNS",
        output_columns=[Column("t1", int, "INTEGER"),
                        Column("t2", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(None,None), (1,None), (None,1)])], exa)
    assert result == [Group([(None,None),(1,None),(None,1)])]
