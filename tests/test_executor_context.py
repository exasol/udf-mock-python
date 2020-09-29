import pytest
from exasol_udf_mock_python.mock_test_executor import *

def test_next_and_emit():
    def udf_wrapper():

        def run(ctx):
            while True:
                ctx.emit(ctx.t)
                if not ctx.next():
                    return

    executor = MockTestExecutor()
    meta = MockMetaData(
            script_code_wrapper_function=udf_wrapper,
            input_type="SET",
            input_columns=[Column("t", int, "INTEGER")],
            output_type="EMITS",
            output_columns=[Column("t", int, "INTEGER")]
            )
    exa = MockExaEnvironment(meta)
    result=executor.run([(1,), (5,), (6,)], exa)
    assert result == [(1,), (5,), (6,)]

def test_get_dataframe_all():
    def udf_wrapper():

        def run(ctx):
            df = ctx.get_dataframe(num_rows='all')
            ctx.emit(df)

    executor = MockTestExecutor()
    meta = MockMetaData(
            script_code_wrapper_function=udf_wrapper,
            input_type="SET",
            input_columns=[Column("t", int, "INTEGER")],
            output_type="EMITS",
            output_columns=[Column("t", int, "INTEGER")]
            )
    exa = MockExaEnvironment(meta)
    result=executor.run([(1,), (5,), (6,)], exa)
    assert result == [(1,), (5,), (6,)]

def test_get_dataframe_iter():
    def udf_wrapper():

        def run(ctx):
            while True:
                df = ctx.get_dataframe(num_rows=2)
                if df is None:
                    return
                else:
                    ctx.emit(df)

    executor = MockTestExecutor()
    meta = MockMetaData(
            script_code_wrapper_function=udf_wrapper,
            input_type="SET",
            input_columns=[Column("t", int, "INTEGER")],
            output_type="EMITS",
            output_columns=[Column("t", int, "INTEGER")]
            )
    exa = MockExaEnvironment(meta)
    result=executor.run([(1,), (2,), (3,), (4,), (5,), (6,)], exa)
    assert result == [(1,), (2,), (3,), (4,), (5,), (6,)]

def test_get_dataframe_iter_next():
    def udf_wrapper():

        def run(ctx):
            while True:
                df = ctx.get_dataframe(num_rows=2)
                if df is None:
                    return
                else:
                    ctx.emit(ctx.data)
                    ctx.next()

    executor = MockTestExecutor()
    meta = MockMetaData(
            script_code_wrapper_function=udf_wrapper,
            input_type="SET",
            input_columns=[Column("t", int, "INTEGER")],
            output_type="RETURNS",
            output_columns=[Column("t", int, "INTEGER")]
            )
    exa = MockExaEnvironment(meta)
    result=executor.run([(1,), (2,), (3,), (4,), (5,), (6,)], exa)
    assert result == [(1,), (2,), (4,), (5,)]
