from enum import Enum


class Types(Enum):
    STR = "String"
    INT = "Integer"
    BOOL = "Boolean"
    JSON = "JSON"
    BINARY = "BINARY"


class SQLObject:
    def __init__(self, column_name, object_type, primary_key=False):
        self.column_name = column_name
        self.object_type = object_type
        self.primary_key = primary_key

    def get_object(self):
        return {"Column": self.column_name, "Type": self.object_type.value, "Primary Key": self.primary_key}


# Defining a table class
Student = [SQLObject("id", Types.INT),
           SQLObject("name", Types.STR, True),
           SQLObject("age", Types.STR),
           SQLObject("department", Types.STR),
           SQLObject("registered", Types.STR)]
