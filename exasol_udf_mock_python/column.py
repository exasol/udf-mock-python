class Column:
    def __init__(self, name, type, sql_type, precision=None, scale=None, length=None):
        self.name = name
        self.type = type
        self.sql_type = sql_type
        self.precision = precision
        self.scale = scale
        self.length = length

    def __repr__(self):
        return str(self.__class__) + ": " + str(self.__dict__)

    def __str__(self):
        return f"{self.name}:{self.type.__name__}"
