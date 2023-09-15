import pytest
import pandas as pd

from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_context import MockContext
from tests.test_mock_context_standalone import meta_set_emits


@pytest.fixture
def context_set_emits(meta_set_emits):
    pets = Group([(1, 'cat'), (2, 'dog')])
    bugs = Group([(3, 'ant'), (4, 'bee'), (5, 'beetle')])
    groups = [pets, bugs]
    return MockContext(iter(groups), meta_set_emits)


def test_scroll(context_set_emits):
    assert context_set_emits._current is None
    assert not context_set_emits._output_groups
    assert context_set_emits._next_group()
    assert context_set_emits.t2 == 'cat'
    assert context_set_emits.next()
    assert context_set_emits.t2 == 'dog'
    assert not context_set_emits.next()
    assert context_set_emits._next_group()
    assert context_set_emits.t2 == 'ant'
    assert context_set_emits.next()
    assert context_set_emits.t2 == 'bee'
    assert context_set_emits.next()
    assert context_set_emits.t2 == 'beetle'
    assert not context_set_emits.next()
    assert not context_set_emits._next_group()
    assert context_set_emits._current is None


def test_output_groups(context_set_emits):
    context_set_emits._next_group()
    context_set_emits.emit(1, 'cat')
    context_set_emits.emit(2, 'dog')
    context_set_emits._next_group()
    context_set_emits.emit(3, 'ant')
    context_set_emits.emit(4, 'bee')
    context_set_emits.emit(5, 'beetle')
    context_set_emits._next_group()
    assert len(context_set_emits._output_groups) == 2
    assert context_set_emits._output_groups[0] == Group([(1, 'cat'), (2, 'dog')])
    assert context_set_emits._output_groups[1] == Group([(3, 'ant'), (4, 'bee'), (5, 'beetle')])


def test_output_groups_partial(context_set_emits):
    context_set_emits._next_group()
    context_set_emits.emit(1, 'cat')
    context_set_emits.emit(2, 'dog')
    context_set_emits._next_group()
    context_set_emits.emit(3, 'ant')
    context_set_emits.emit(4, 'bee')
    assert len(context_set_emits._output_groups) == 2
    assert context_set_emits._output_groups[0] == Group([(1, 'cat'), (2, 'dog')])
    assert context_set_emits._output_groups[1] == Group([(3, 'ant'), (4, 'bee')])
