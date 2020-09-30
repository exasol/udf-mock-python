from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.connection import Connection
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.udf_mock_executor import UDFMockExecutor


def test_exa_meta_in_init():
    def udf_wrapper():
        script_code = exa.meta.script_code

        def run(ctx):
            ctx.emit(script_code)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", str, "VARCHAR(2000)")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,)])], exa)
    assert result == [Group([(exa.meta.script_code,)])]


def test_exa_meta_in_run():
    def udf_wrapper():
        def run(ctx):
            ctx.emit(exa.meta.script_code)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", str, "VARCHAR(2000)")]
    )
    exa = MockExaEnvironment(meta)
    result = executor.run([Group([(1,)])], exa)
    assert result == [Group([(exa.meta.script_code,)])]


def test_get_connection_in_init():
    def udf_wrapper():
        con = exa.get_connection("TEST_CON")

        def run(ctx):
            ctx.emit(con.address)

    executor = UDFMockExecutor()
    meta = MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="EMITS",
        output_columns=[Column("t", str, "VARCHAR(2000)")]
    )
    exa = MockExaEnvironment(meta, connections={"TEST_CON": Connection(address="https://test.de")})
    result = executor.run([Group([(1,)])], exa)
    assert result == [Group([("https://test.de",)])]
