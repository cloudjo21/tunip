import numpy as np

from pydantic import BaseModel
from pydantic._internal._model_construction import ModelMetaclass
from typing import Dict, Optional


class RecordNumpyDtypeInferer:

    @staticmethod
    def infer_from_model_klass(record_klass: ModelMetaclass) -> Dict[str, np.dtype]:

        field2dtype = dict()

        for field_name in record_klass.model_fields:
            np_dtype = np.dtype(record_klass.model_fields[field_name].annotation)
            field2dtype.update({field_name:np_dtype})
        return field2dtype

    @staticmethod
    def infer_from_model(record: BaseModel) -> Dict[str, np.dtype]:
        schema_dict = record.model_json_schema()

        field2dtype = dict()

        for field_name in schema_dict["properties"]:
            field_type = schema_dict["properties"][field_name]["type"]

            np_dtype: Optional[np.dtype] = None
            # numpy - one-character type strings
            if field_type == "number":
                np_dtype = np.dtype("f")
            elif field_type == "integer":
                np_dtype = np.dtype("i")
            elif field_type == "string":
                np_dtype = np.dtype("U")
            else:
                # boolean
                np_dtype = np.dtype("?")
            
            field2dtype.update({field_name:np_dtype})

        return field2dtype
