import pytest
import pandas as pd

from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.mock_context import StandaloneMockContext


def udf_wrapper():
    pass


@pytest.fixture
def meta_scalar_return():
    return MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type='SCALAR',
        input_columns=[Column('t', int, 'INTEGER')],
        output_type='RETURNS',
        output_columns=[Column('t', int, 'INTEGER')]
    )


@pytest.fixture
def meta_set_emits():
    return MockMetaData(
        script_code_wrapper_function=udf_wrapper,
        input_type='SET',
        input_columns=[Column('t1', int, 'INTEGER'), Column('t2', str, 'VARCHAR(100)')],
        output_type='EMITS',
        output_columns=[Column('t1', int, 'INTEGER'), Column('t2', str, 'VARCHAR(100)')]
    )


@pytest.fixture
def context_scalar_return(meta_scalar_return):
    return StandaloneMockContext((5,), meta_scalar_return)


@pytest.fixture
def context_set_emits(meta_set_emits):
    return StandaloneMockContext([(5, 'abc'), (6, 'efgh')], meta_set_emits)


def test_get_dataframe(context_set_emits):
    df = context_set_emits.get_dataframe()
    expected_df = pd.DataFrame({'t1': [5, 6], 't2': ['abc', 'efgh']})
    pd.testing.assert_frame_equal(df, expected_df)


def test_get_dataframe_limited(context_set_emits):
    df = context_set_emits.get_dataframe(1, 1)
    expected_df = pd.DataFrame({'t2': ['abc']})
    pd.testing.assert_frame_equal(df, expected_df)


def test_attr_set(context_set_emits):
    assert context_set_emits.t1 == 5
    assert context_set_emits.t2 == 'abc'


def test_attr_scalar(context_scalar_return):
    assert context_scalar_return.t == 5


def test_next(context_set_emits):
    assert context_set_emits.next()
    assert context_set_emits.t1 == 6
    assert context_set_emits.t2 == 'efgh'


def test_next_end(context_set_emits):
    assert context_set_emits.next()
    assert not context_set_emits.next()


def test_reset(context_set_emits):
    assert context_set_emits.next()
    context_set_emits.reset()
    assert context_set_emits.t1 == 5
    assert context_set_emits.t2 == 'abc'


def test_size(context_set_emits):
    assert context_set_emits.size() == 2


def test_validate_tuples_good(meta_set_emits):
    StandaloneMockContext._validate_tuples((10, 'fish'), meta_set_emits.output_columns)


def test_validate_tuples_bad(meta_set_emits):
    with pytest.raises(Exception):
        StandaloneMockContext._validate_tuples((10,), meta_set_emits.output_columns)
    with pytest.raises(Exception):
        StandaloneMockContext._validate_tuples((10, 'fish', 4.5), meta_set_emits.output_columns)
    with pytest.raises(Exception):
        StandaloneMockContext._validate_tuples((10., 'fish'), meta_set_emits.output_columns)


def test_emit_df(context_set_emits):
    df = pd.DataFrame({'t1': [1, 2], 't2': ['cat', 'dog']})
    context_set_emits.emit(df)
    assert context_set_emits.output == [(1, 'cat'), (2, 'dog')]
