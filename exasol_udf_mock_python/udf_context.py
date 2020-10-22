from abc import ABCMeta, abstractmethod
from typing import Union, Optional

import pandas as pd


class UDFContext(metaclass=ABCMeta):
    """
    UDFContext used to iterate over the input rows of the UDF and to emit output rows back to the database.
    The columns of the input rows are accessible by their name as attributes of the UDFContext, e.g. ctx.a,
    for the column "a".
    """

    @abstractmethod
    def get_dataframe(self, num_rows: Union[str, int], start_col: int = 0) -> Optional[pd.DataFrame]:
        """
        Returns the next input rows as a pandas dataframe. This function is only available for SET UDFs.
        :param num_rows: either the string "all", or the number of rows to return as int
        :param start_col: determines from which column on the dataframe contains columns
        :return: A pandas Dataframe or None, if there are now more rows
        """
        pass

    @abstractmethod
    def next(self, reset: bool = False) -> bool:
        """
        Advances the Context by one row. This function is only available for SET UDFs.
        :param reset: Resets the context. After the reset the context stays at the first row
        :return: True, if the Context advanced by one row, False if the Context were already at the last row
        """
        pass

    @abstractmethod
    def size(self) -> int:
        """
        Returns the number of input rows. This function is only available for SET UDFs.
        :return:
        """
        pass

    @abstractmethod
    def reset(self):
        """
        Resets the context. After the reset the context stays at the first row. This function is only available for SET UDFs.
        """
        pass

    @abstractmethod
    def emit(self, *args):
        """
        Emits output rows to the database. This function is only available for EMITS UDFs.
        :param args: Either, one argument per output column or a single pandas DataFrame
        """
        pass
