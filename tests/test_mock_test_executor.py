import pytest
from exasol_udf_mock_python.mock_test_executor import *

def test_exa_meta_in_init():
    def udf_wrapper():
        
        script_code = exa.meta.script_code

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

def test_exa_meta_in_run():
    def udf_wrapper():
        
        def run(ctx):
            script_code = exa.meta.script_code

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

def test_get_connection_in_init():
    def udf_wrapper():
        
        con = exa.get_connection("TEST_CON")

        def run(ctx):
            ctx.emit(con.address)

    executor = MockTestExecutor()
    meta = MockMetaData(
            script_code_wrapper_function=udf_wrapper,
            input_type="SET",
            input_columns=[Column("t", int, "INTEGER")],
            output_type="EMITS",
            output_columns=[Column("t", str, "VARCHAR(2000)")]
            )
    exa = MockExaEnvironment(meta,connections={"TEST_CON": Connection(address="https://test.de")})
    result=executor.run([], exa)
    assert result == [("https://test.de",)]

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
