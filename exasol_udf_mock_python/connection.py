from enum import Enum


class ConnectionType(Enum):
    PASSWORD = 1


class Connection:
    def __init__(self, address: str, user: str = None, password: str = None,
                 type: ConnectionType = ConnectionType.PASSWORD):
        self.type = type
        self.password = password
        self.user = user
        self.address = address

    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__)