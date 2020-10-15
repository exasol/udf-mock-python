import itertools
from typing import List, Tuple, Iterator

import pandas as pd

from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.group import Group
from exasol_udf_mock_python.mock_meta_data import MockMetaData


class MockContext:

    def __init__(self, input_groups: Iterator[Group], metadata: MockMetaData):
        self._input_groups = input_groups
        self._output_groups = []
        self._input_group = None  # type: Group
        self._output_group_list = None # type: List
        self._output_group = None  # type: Group
        self._iter = None  # type: Iterator[Tuple]
        self._len = None # type: int
        self._metadata = metadata
        self._name_position_map = \
            {column.name: position
             for position, column
             in enumerate(metadata.input_columns)}

    def _next_group(self):
        try:
            self._input_group = next(self._input_groups)
        except StopIteration as e:
            self._data = None
            self._output_group_list = None
            self._output_group = None
            self._input_group = None
            self._iter = None
            self._len = None
            return False
        self._len = len(self._input_group)
        if self._len == 0:
            self._data = None
            self._output_group_list = None
            self._output_group = None
            self._input_group = None
            self._iter = None
            self._len = None
            raise RuntimeError("Empty input groups are not allowd")
        self._output_group_list = []
        self._output_group = Group(self._output_group_list)
        self._output_groups.append(self._output_group)
        self._iter = iter(self._input_group)
        self.next()
        return True

    def get_dataframe(self, num_rows='all', start_col=0):
        if self._data is None:
            return None
        columns_ = [column.name for column in self._metadata.input_columns]

        i = 0
        df = None
        while num_rows == 'all' or i < num_rows:
            df_current = pd.DataFrame.from_records([self._data], columns=columns_)
            if df is None:
                df = df_current
            else:
                df = df.append(df_current)
            if not self.next():
                break
            i+=1
        if df is not None:
            df = df.reset_index(drop=True)
        return df

    def __getattr__(self, name):
        return self._data[self._name_position_map[name]]

    def next(self, reset:bool = False):
        if reset:
            self.reset()
        else:
            try:
                new_data = next(self._iter)
                self._data = new_data
                self.validate_tuples(self._data, self._metadata.input_columns)
                return True
            except StopIteration as e:
                self._data = None
                return False

    def size(self):
        return self._len

    def reset(self):
        self._iter = iter(self._input_group)
        self.next()

    def emit(self, *args):
        if len(args) == 1 and isinstance(args[0], pd.DataFrame):
            tuples = [tuple(x) for x in args[0].astype('object').values]
        else:
            tuples = [args]
        for row in tuples:
            self.validate_tuples(row, self._metadata.output_columns)
        self._output_group_list.extend(tuples)
        return

    def validate_tuples(self, row: Tuple, columns: List[Column]):
        if len(row) != len(columns):
            raise Exception(f"row {row} has not the same number of values as columns are defined")
        for i, column in enumerate(columns):
            if row[i] is not None and not isinstance(row[i], column.type):
                raise TypeError(f"Value {row[i]} ({type(row[i])}) at position {i} is not a {column.type}")
