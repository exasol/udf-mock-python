import pytest

from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.udf_mock_executor import UDFMockExecutor


def test_next_and_emit():
    def udf_wrapper():

        def run(ctx):
            while True:
                ctx.emit(ctx.t)
                if not ctx.next():
                    return

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,), (5,), (6,)])], exa)
    assert result == [Group([(1,), (5,), (6,)])]

def test_emit_single_column_none():
    def udf_wrapper():

        def run(ctx):
            ctx.emit(None)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,), (5,), (6,)])], exa)
    assert result == [Group([(None,)])]

def test_emit_multi_column_none():
    def udf_wrapper():

        def run(ctx):
            ctx.emit(None,None)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t1", int, "INTEGER"),
                        Column("t2", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,), (5,), (6,)])], exa)
    assert result == [Group([(None,None)])]

def test_next_emit_reset():
    def udf_wrapper():

        def run(ctx):
            while True:
                ctx.emit(ctx.t)
                if not ctx.next():
                    break
            ctx.reset()
            while True:
                ctx.emit(ctx.t+1)
                if not ctx.next():
                    break

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,), (5,), (6,)])], exa)
    assert result == [Group([(1,), (5,), (6,), (2,), (6,), (7,)])]

def test_next_reset_combined():
    def udf_wrapper():

        def run(ctx):
            for i in range(2):
                ctx.emit(ctx.t)
                if not ctx.next():
                    break
            ctx.next(reset=True)
            for i in range(2):
                ctx.emit(ctx.t+1)
                if not ctx.next():
                    break

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,), (5,), (6,)])], exa)
    assert result == [Group([(1,), (5,),(2,), (6,)])]


def test_get_dataframe_all():
    def udf_wrapper():
        def run(ctx):
            df = ctx.get_dataframe(num_rows='all')
            ctx.emit(df)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,), (5,), (6,)])], exa)
    assert result == [Group([(1,), (5,), (6,)])]


def test_get_dataframe_iter():
    def udf_wrapper():

        def run(ctx):
            while True:
                df = ctx.get_dataframe(num_rows=2)
                if df is None:
                    return
                else:
                    ctx.emit(df)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,), (2,), (3,), (4,), (5,), (6,)])], exa)
    assert result == [Group([(1,), (2,), (3,), (4,), (5,), (6,)])]


def test_get_dataframe_iter_next():
    def udf_wrapper():

        def run(ctx):
            while True:
                df = ctx.get_dataframe(num_rows=2)
                if df is None:
                    return
                else:
                    ctx.emit(df)
                    ctx.next()

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,), (2,), (3,), (4,), (5,), (6,)])], exa)
    assert result == [Group([(1,), (2,), (4,), (5,)])]

def test_emit_tuple_exception():
    def udf_wrapper():

        def run(ctx):
            while True:
                ctx.emit((1,))

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    with pytest.raises(TypeError):
        result = executor.run([Group([(1,), (2,), (3,), (4,), (5,), (6,)])], exa)
