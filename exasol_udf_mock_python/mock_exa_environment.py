from typing import Dict

from exasol_udf_mock_python.connection import Connection
from exasol_udf_mock_python.mock_meta_data import MockMetaData


class MockExaEnvironment:
    def __init__(self,
                 metadata: MockMetaData,
                 connections: Dict[str, Connection] = None
                 ):
        self._connections = connections
        if self._connections is None:
            self._connections = {}
        self._metadata = metadata

    def get_connection(self, name: str) -> Connection:
        return self._connections[name]

    def import_script(self, schema_script_name: str):
        raise Exception(
            "Import Script is currently not supported by this mock. We also advise against its usage in general and use instead proper packaging mechanism of your the language in choice.")

    @property
    def meta(self):
        return self._metadata

    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__)
