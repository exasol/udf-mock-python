import itertools
from typing import List, Tuple, Any

import pandas as pd

from exasol_udf_mock_python.mock_meta_data import MockMetaData


class MockContext:

    def __init__(self, inputs:List[Tuple[Any]], metadata: MockMetaData):
        self._inputs = inputs                               # actual data
        self._outputs = []
        self._iter = iter(self._inputs)
        self.next()
        self._metadata = metadata
        self._name_position_map = \
            {column.name: position
             for position, column
             in enumerate(metadata.input_columns)}

    def get_dataframe(self, num_rows='all', start_col=0):
        if self._data is None:
            return None
        if num_rows == 'all':
            iter = self._iter
        else:
            iter = itertools.islice(self._iter, num_rows - 1)
        columns_ = [column.name for column in self._metadata.input_columns]
        df_next = pd.DataFrame.from_records(data=iter, columns=columns_)
        df_current = pd.DataFrame.from_records([self._data], columns=columns_)
        df = df_current.append(df_next)
        df = df.reset_index(drop=True)
        if df.empty:
            return None
        else:
            self.next()
            return df

    @property
    def data(self):
        return self._data

    def __getattr__(self, name):
        return self._data[self._name_position_map[name]]

    def next(self):
        try:
            new_data = next(self._iter)
            self._data = new_data
            return True
        except StopIteration as e:
            self._data = None
            return False

    def size(self):
        return len(self._inputs)

    def reset(self):
        self._iter = iter(self._inputs)
        self.next()

    def emit(self, *args):
        self._outputs.append(args)
        return