import collections.abc
from abc import abstractmethod
from typing import List, Tuple, Iterable, Set, Iterator


class IterableWithSize(collections.abc.Iterable):
    @abstractmethod
    def __len__(self):
        return None


class Group(collections.abc.Iterable):
    """
    Represents a Group (SET Function) or a Batch (Scalar Function) of rows
    """

    def __init__(self, rows: Iterable[Tuple]):
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)

    def __len__(self):
        if isinstance(self._rows, (List, Set, Tuple, IterableWithSize)):
            return len(self._rows)
        else:
            return sum(1 for _ in self._rows)

    @property
    def rows(self) -> List[Tuple]:
        """
        This property transforms the Iterable of rows into a List
        None: This can potentially can cause the materialization of a large list.
        :return: The rows of this group as a list
        """
        return list(iter(self._rows))

    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

    def __eq__(self, other: "Group"):
        if not isinstance(other, Group):
            return False
        self_iter = iter(self)
        other_iter = iter(other)
        return self._compare_iter(self_iter, other_iter)

    def _compare_iter(self, self_iter: Iterator[Tuple], other_iter: Iterator[Tuple]):
        self_at_end = False
        try:
            while True:
                try:
                    self_row = next(self_iter)
                except StopIteration as e:
                    self_at_end = True
                    break
                other_row = next(other_iter)
                if self_row != other_row:
                    return False
            other_row = next(other_iter)
        except StopIteration as e:
            if self_at_end:
                return True
            else:
                return False
