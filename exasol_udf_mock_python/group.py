from typing import List, Tuple


class Group:
    """
    Reperesents a Group (SET Function) or a Batch (Scalar Function) of rows
    """

    def __init__(self, rows:List[Tuple]):
        self.rows = rows

    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

    def __eq__(self, other):
        return self.__dict__ == other.__dict__