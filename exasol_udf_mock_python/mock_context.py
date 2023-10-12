from typing import List, Tuple, Iterator, Iterable, Any, Optional, Union

import pandas as pd

from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_meta_data import MockMetaData
from exasol_udf_mock_python.udf_context import UDFContext


class MockContext(UDFContext):
    """
    Implementation of generic UDF Mock Context interface for a SET UDF with groups.
    This class allows iterating over groups. The functionality of the UDF Context are applicable
    for the current input group.

    Call `_next_group` to iterate over groups. The `_output_groups` property provides the emit
    output for all groups iterated so far including the output for the current group.
    """

    def __init__(self, input_groups: Iterator[Group], metadata: MockMetaData):
        """
        :param input_groups:    Input groups. Each group object should contain input rows for the group.

        :param metadata:        The mock metadata object.
        """

        self._input_groups = input_groups
        self._metadata = metadata
        """ Mock context for the current group """
        self._current:Optional[StandaloneMockContext] = None
        """ Output for all groups """
        self._previous_groups: List[Group] = []

    def _next_group(self) -> bool:
        """
        Moves group iterator to the next group.
        Returns False if the iterator gets beyond the last group. Returns True otherwise.
        """

        # Save output of the current group
        if self._current is not None:
            self._previous_groups.append(Group(self._current.output))
            self._current = None

        # Try get to the next input group
        try:
            input_group = next(self._input_groups)
        except StopIteration as e:
            return False
        if len(input_group) == 0:
            raise RuntimeError("Empty input groups are not allowed")

        # Create Mock Context for the new input group
        self._current = StandaloneMockContext(input_group, self._metadata)
        return True

    @property
    def _output_groups(self):
        """
        Output of all groups including the current one.
        """
        if self._current is None:
            return self._previous_groups
        else:
            groups = list(self._previous_groups)
            groups.append(Group(self._current.output))
            return groups

    def __getattr__(self, name):
        return None if self._current is None else getattr(self._current, name)

    def get_dataframe(self, num_rows: Union[str, int], start_col: int = 0) -> Optional[pd.DataFrame]:
        return None if self._current is None else self._current.get_dataframe(num_rows, start_col)

    def next(self, reset: bool = False) -> bool:
        return False if self._current is None else self._current.next(reset)

    def size(self) -> int:
        return 0 if self._current is None else self._current.size()

    def reset(self) -> None:
        if self._current is not None:
            self._current.reset()

    def emit(self, *args):
        if self._current is not None:
            self._current.emit(*args)


class StandaloneMockContext(UDFContext):
    """
    Implementation of generic UDF Mock Context interface a SCALAR UDF or a SET UDF with no groups.

    For Emit UDFs the output in the form of the list of tuples can be
    access by reading the `output` property.
    """

    def __init__(self, inp: Any, metadata: MockMetaData):
        """
        :param  inp:        Input rows for a SET UDF or parameters for a SCALAR one.
                            In the former case the input object must be an iterable of rows. This, for example,
                            can be a Group object. It must implement the __len__ method. Each data row must be
                            an indexable container, e.g. a tuple.
                            In the SCALAR case the input can be a scalar value, or tuple. This can also be wrapped
                            in an iterable container, similar to the SET case.

        :param metadata:    The mock metadata object.
        """

        if metadata.input_type.upper() == 'SCALAR':
            # Figure out if the SCALAR parameters are provided as a scalar value or a tuple
            # and also if there is a wrapping container around. In any case, this should be
            # converted to a form [(param1[, param2, ...)]
            if isinstance(inp, Iterable) and not isinstance(inp, str):
                row1 = next(iter(inp))
                if isinstance(row1, Iterable) and not isinstance(row1, str):
                    self._input = inp
                else:
                    self._input = [inp]
            else:
                self._input = [(inp,)]
        else:
            self._input = inp
        self._metadata = metadata
        self._data = None       # type: Optional[Any]
        self._iter = None       # type: Optional[Iterator[Tuple[Any, ...]]]
        self._name_position_map = \
            {column.name: position
             for position, column
             in enumerate(metadata.input_columns)}
        self._output = []
        self.next(reset=True)

    @property
    def output(self) -> List[Tuple[Any, ...]]:
        """Emitted output so far"""
        return self._output

    @staticmethod
    def _is_positive_integer(value):
        return value is not None and isinstance(value, int) and value > 0

    def get_dataframe(self, num_rows='all', start_col=0):
        if not (num_rows == 'all' or self._is_positive_integer(num_rows)):
            raise RuntimeError("get_dataframe() parameter 'num_rows' must be 'all' or an integer > 0")
        if not (self._is_positive_integer(start_col) or start_col == 0):
            raise RuntimeError("get_dataframe() parameter 'start_col' must be an integer >= 0")
        if self._data is None:
            return None
        columns_ = [column.name for column in self._metadata.input_columns[start_col:]]

        i = 0
        df = None
        while num_rows == 'all' or i < num_rows:
            df_current = pd.DataFrame.from_records(
                [self._data[start_col:]], columns=columns_)
            if df is None:
                df = df_current
            else:
                df = df.append(df_current)
            if not self.next():
                break
            i += 1
        if df is not None:
            df = df.reset_index(drop=True)
        return df

    def __getattr__(self, name):
        return None if self._data is None else self._data[self._name_position_map[name]]

    def next(self, reset: bool = False):
        if self._iter is None or reset:
            self.reset()
        else:
            try:
                new_data = next(self._iter)
                self._data = new_data
                self._validate_tuples(self._data, self._metadata.input_columns)
                return True
            except StopIteration as e:
                self._data = None
                return False

    def size(self):
        return len(self._input)

    def reset(self):
        self._iter = iter(self._input)
        self.next()

    def emit(self, *args):
        if len(args) == 1 and isinstance(args[0], pd.DataFrame):
            tuples = [tuple(x) for x in args[0].astype('object').values]
        else:
            tuples = [args]
        for row in tuples:
            self._validate_tuples(row, self._metadata.output_columns)
        self._output.extend(tuples)

    @staticmethod
    def _validate_tuples(row: Tuple, columns: List[Column]):
        if len(row) != len(columns):
            raise Exception(f"row {row} has not the same number of values as columns are defined")
        for i, column in enumerate(columns):
            if row[i] is not None and not isinstance(row[i], column.type):
                raise TypeError(f"Value {row[i]} ({type(row[i])}) at position {i} is not a {column.type}")
