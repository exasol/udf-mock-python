import pytest
import unittest
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
            ctx.emit(None, None)

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
    assert result == [Group([(None, None)])]


def test_next_emit_reset():
    def udf_wrapper():

        def run(ctx):
            while True:
                ctx.emit(ctx.t)
                if not ctx.next():
                    break
            ctx.reset()
            while True:
                ctx.emit(ctx.t + 1)
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
                ctx.emit(ctx.t + 1)
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
    assert result == [Group([(1,), (5,), (2,), (6,)])]


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


def test_get_dataframe_num_rows_1():
    def udf_wrapper():
        def run(ctx):
            df = ctx.get_dataframe(num_rows=1)
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
    assert result == [Group([(1,), ])]


def test_get_dataframe_num_rows_0():
    def udf_wrapper():
        def run(ctx):
            df = ctx.get_dataframe(num_rows=0)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    with pytest.raises(RuntimeError) as excinfo:
        result = executor.run([Group([(1,), (2,), (3,), (4,), (5,), (6,)])], exa)


def test_get_dataframe_num_rows_float():
    def udf_wrapper():
        def run(ctx):
            df = ctx.get_dataframe(num_rows=1.5)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    with pytest.raises(RuntimeError) as excinfo:
        result = executor.run([Group([(1,), (2,), (3,), (4,), (5,), (6,)])], exa)

def test_get_dataframe_num_rows_None():
    def udf_wrapper():
        def run(ctx):
            df = ctx.get_dataframe(num_rows=None)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    with pytest.raises(RuntimeError) as excinfo:
        result = executor.run([Group([(1,), (2,), (3,), (4,), (5,), (6,)])], exa)


def test_get_dataframe_num_rows_negative():
    def udf_wrapper():
        def run(ctx):
            df = ctx.get_dataframe(num_rows=-1)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    with pytest.raises(RuntimeError) as excinfo:
        result = executor.run([Group([(1,), (2,), (3,), (4,), (5,), (6,)])], exa)

def test_get_dataframe_start_col_None():
    def udf_wrapper():
        def run(ctx):
            df = ctx.get_dataframe(num_rows=10, start_col=None)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    with pytest.raises(RuntimeError) as excinfo:
        result = executor.run([Group([(1,), (2,), (3,), (4,), (5,), (6,)])], exa)

def test_get_dataframe_start_col_negative():
    def udf_wrapper():
        def run(ctx):
            df = ctx.get_dataframe(num_rows=10, start_col=-1)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", int, "INTEGER")]
    )
    exa = MockExaEnvironment(meta)
    with pytest.raises(RuntimeError) as excinfo:
        result = executor.run([Group([(1,), (2,), (3,), (4,), (5,), (6,)])], exa)

def test_get_dataframe_start_col_0():
    def udf_wrapper():
        def run(ctx):
            df = ctx.get_dataframe(num_rows=1, start_col=0)
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
    assert result == [Group([(1,), ])]

def test_get_dataframe_start_col_positive():
    def udf_wrapper():
        def run(ctx):
            df = ctx.get_dataframe(num_rows=1, start_col=1)
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
    assert result == [Group([(1,), ])]

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


def test_access_inputs_by_index():
    def udf_wrapper():
        def run(ctx):
            ctx.emit(ctx[0])
            ctx.emit(ctx[1])
            ctx.emit(ctx[2])

    input_columns = [Column("t1", int, "INTEGER"),
                     Column("t2", int, "INTEGER"),
                     Column("t3", int, "INTEGER")]
    output_columns = [Column("o1", int, "INTEGER")]
    for is_variadic in [True, False]:
        meta = MockMetaData(
            script_code_wrapper_function=udf_wrapper,
            input_type="SET",
            input_columns=input_columns,
            output_type="EMITS",
            output_columns=output_columns,
            is_variadic=is_variadic
        )
        input_data = [(1, 2, 3), (4, 5, 6)]
        exa = MockExaEnvironment(meta)
        executor = UDFMockExecutor()
        result = executor.run([Group(input_data)], exa)
        for i, group in enumerate(result):
            result_row = group.rows
            assert len(result_row) == len(input_columns)
            for j in range(len(result_row)):
                assert len(result_row[j]) == len(output_columns)
                assert input_data[i][j] == result_row[j][0]


class InvalidAccessToVariadicInputs(unittest.TestCase):
    def test_access_variadic_inputs_by_name(self):
        def udf_wrapper():
            def run(ctx):
                ctx.emit(ctx.t1)

        input_columns = [Column("t1", int, "INTEGER")]
        output_columns = [Column("o1", int, "INTEGER")]
        meta = MockMetaData(
            script_code_wrapper_function=udf_wrapper,
            input_type="SET",
            input_columns=input_columns,
            output_type="EMITS",
            output_columns=output_columns,
            is_variadic=True)

        input_data = [(1,)]
        exa = MockExaEnvironment(meta)
        executor = UDFMockExecutor()
        with self.assertRaises(RuntimeError):
            result = executor.run([Group(input_data)], exa)
