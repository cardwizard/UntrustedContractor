from enum import Enum
from typing import List
from json import dumps


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


class SQLSchema:
    def __init__(self, object_list: List[SQLObject])->None:
        self.object_list = object_list

    def get_schema(self) -> str:
        return dumps([x.get_object() for x in self.object_list])