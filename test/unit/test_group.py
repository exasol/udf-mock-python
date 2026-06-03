from collections.abc import Iterable

from exasol_udf_mock_python.group import Group, IterableWithSize


def test_groups_are_equal():
    group1 = Group([(1,), (2,), (3,)])
    group2 = Group([(1,), (2,), (3,)])
    assert group1 == group2


def test_group_prefix_equal_but_first_group_is_longer():
    group1 = Group([(1,), (2,), (3,), (4,)])
    group2 = Group([(1,), (2,), (3,)])
    assert group1 != group2


def test_group_prefix_equal_but_second_group_is_longer():
    group1 = Group([(1,), (2,), (3,)])
    group2 = Group([(1,), (2,), (3,), (4,)])
    assert group1 != group2


def test_group_same_length_difference_in_the_middle():
    group1 = Group([(1,), (2,), (5,), (4,)])
    group2 = Group([(1,), (2,), (3,), (4,)])
    assert group1 != group2


def test_group_has_tuple_as_iterable_but_rows_is_list():
    group = Group(((1,), (2,), (5,), (4,)))
    assert group.rows == [(1,), (2,), (5,), (4,)]


def test_group_len():
    group = Group(((1,), (2,), (5,), (4,)))
    assert len(group) == 4


def test_group_iter():
    group = Group(((1,), (2,), (5,), (4,)))
    assert list(iter(group)) == [(1,), (2,), (5,), (4,)]


class MyIterable(Iterable):
    def __iter__(self):
        return iter([(1,), (2,), (3,)])


def test_group_with_custom_iterable_rows():
    group = Group(MyIterable())
    assert group.rows == [(1,), (2,), (3,)]


def test_group_with_custom_iterable_len():
    group = Group(MyIterable())
    assert len(group) == 3


def test_group_with_iterable_with_size_len():
    class MyIterableWithSize(IterableWithSize):
        def __iter__(self):
            raise Exception("The group should use __len__ instead of __iter__ to determine the length")

        def __len__(self):
            return 3

    group = Group(MyIterableWithSize())
    assert len(group) == 3
