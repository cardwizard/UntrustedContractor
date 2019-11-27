from Utils.models.schemas import SQLObject, Types, SQLSchema
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
    def _int_projections_2(data, splits):
        """
        Data needs to have just 2 keys - id and field
        """
        final_output = []

        for split in splits:
            cur_split_matrix = {"start": split[0], "end": split[1], "proj_id_list": []}

            for d in data:
                if split[0] <= d["column"] < split[1]:
                    cur_split_matrix["proj_id_list"].append(d["proj_id"])

            final_output.append(cur_split_matrix)

        return final_output

    @staticmethod
    def _str_projections_2(data, splits):
        final_output = []

        for split in splits:
            cur_split_matrix = {"startswith": split, "proj_id_list": []}

            for d in data:
                if d["column"].startswith(split):
                    cur_split_matrix["proj_id_list"].append(d["proj_id"])

            final_output.append(cur_split_matrix)

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

    def create_projections_2(self, data, splits):

        if self.data_type == "int":
            schema = SQLSchema([SQLObject("split_id", Types.INT, True, True),
                                SQLObject("start", Types.INT),
                                SQLObject("end", Types.INT),
                                SQLObject("proj_id_list", Types.JSON)])
            return schema.get_schema(), self._int_projections_2(data, splits)

        if self.data_type == "str":
            schema = SQLSchema([SQLObject("split_id", Types.INT, True, True),
                                SQLObject("startswith", Types.STR),
                                SQLObject("proj_id_list", Types.JSON)])
            return schema.get_schema(), self._str_projections_2(data, splits)

        if self.data_type == "identity":
            schema = SQLSchema([SQLObject("split_id", Types.INT, True, True),
                                SQLObject("value", Types.STR),
                                SQLObject("proj_id_list", Types.JSON)])

            return schema.get_schema(), [{"value": d["column"], "proj_id_list": [d["proj_id"]]}
                                         for d in data]

    def create_projections(self, data, splits):

        if self.data_type == "int":
            schema = SQLSchema([SQLObject("proj_id", Types.INT, True),
                                SQLObject("start", Types.INT),
                                SQLObject("end", Types.INT)])
            return schema.get_schema(), self._int_projections(data, splits)

        if self.data_type == "str":
            schema = SQLSchema([SQLObject("proj_id", Types.INT, True),
                                SQLObject("startswith", Types.STR)])
            return schema.get_schema(), self._str_projections(data, splits)

        if self.data_type == "identity":
            schema = SQLSchema([SQLObject("value", Types.STR),
                                SQLObject("proj_id", Types.INT, True)])

            return schema.get_schema(), [{"value": d["column"], "proj_id": d["proj_id"]}
                                         for d in data]

