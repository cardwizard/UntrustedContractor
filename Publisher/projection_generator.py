from Utils.models.schemas import SQLObject, Types
from json import dumps

class Projection:
    def __init__(self, data_type):
        self.data_type = data_type

    @staticmethod
    def _int_projections(data, splits):
        """
        Data needs to have just 2 keys - id and field
        """
        final_output = []

        for d in data:
            marked = False

            for split in splits:
                if not marked and split[0] <= d["column"] < split[1]:
                    final_output.append({"start": split[0], "end": split[1], "proj_id": d["proj_id"]})
                    marked = True

        return final_output

    @staticmethod
    def _str_projections(data, splits):
        final_output = []

        for d in data:
            marked = False

            for split in splits:
                if not marked and d["column"].startswith(split):
                    final_output.append({"startswith": split, "proj_id": d["proj_id"]})
                    marked = True
        return final_output

    def create_projections(self, data, splits):

        if self.data_type == "int":
            schema = [SQLObject("start", Types.INT), SQLObject("end", Types.INT), SQLObject("proj_id", Types.INT, True)]
            return dumps([x.get_object() for x in schema]), self._int_projections(data, splits)
        if self.data_type == "str":
            schema = [SQLObject("startswith", Types.STR), SQLObject("proj_id", Types.INT, True)]
            return dumps([x.get_object() for x in schema]), self._str_projections(data, splits)

        if self.data_type == "identity":
            schema = [SQLObject("value", Types.STR), SQLObject("proj_id", Types.INT, True)]
            return dumps([x.get_object() for x in schema]), [{"value": d["column"], "proj_id": d["proj_id"]} for d in data]
