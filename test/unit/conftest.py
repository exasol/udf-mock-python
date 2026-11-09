import pytest

from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.mock_meta_data import MockMetaData


def udf_wrapper():
    pass


@pytest.fixture
def meta_scalar_return():
    return MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SCALAR",
        input_columns=[Column("t", int, "INTEGER")],
        output_type="RETURNS",
        output_columns=[Column("t", int, "INTEGER")],
    )


@pytest.fixture
def meta_set_emits():
    return MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type="SET",
        input_columns=[Column("t1", int, "INTEGER"), Column("t2", str, "VARCHAR(100)")],
        output_type="EMITS",
        output_columns=[
            Column("t1", int, "INTEGER"),
            Column("t2", str, "VARCHAR(100)"),
        ],
    )
