import numpy as np
import orjson
import pandas as pd

from abc import abstractmethod
from pydantic import BaseModel, ValidationError, create_model
from pydantic._internal._model_construction import ModelMetaclass
from typing import Any, Dict, Optional

from tunip.file_utils import services as file_services
from tunip.service_config import ServiceLevelConfig


class InputFeatureInstance(BaseModel):
    pass


class FeaturedSchema:

    def __init__(self, schema: ModelMetaclass):
        self.schema: ModelMetaclass = schema

    def accept(self, feature_instance: dict, strict: bool=True) -> bool:
        # self.schema.model_json_schema().get("properties").keys()
        accept_status = True
        try:
            self.schema.model_validate(feature_instance, strict)
        except ValidationError as ve:
            accept_status = False
        finally:
            return accept_status

    def instantiate(self, **kwargs):
        return self.schema(**kwargs)


class CandidateFeatureInstance(BaseModel):
    pass


class StaticFeaturedSchema(FeaturedSchema):

    def __init__(self, schema: ModelMetaclass, input_feature_instance: InputFeatureInstance):
        super(StaticFeaturedSchema, self).__init__(schema)
        self.input_feature_instance = input_feature_instance

    # def accept(self, typed_schema: dict):
    #     pass


class DynamicFeaturedSchema(FeaturedSchema):
    
    def __init__(self, schema: ModelMetaclass):
        super(DynamicFeaturedSchema, self).__init__(schema)

    # def accept(self, typed_schema: dict):
    #     pass


class FeaturedSchemaBasedModel:

    def __init__(self, featured_schema: FeaturedSchema, use_static_schema: bool=True):
        self.featured_schema = featured_schema
        self.use_static_schema = use_static_schema

    def accept_candidate(self, feature_instance: CandidateFeatureInstance) -> bool:
        accept_result = None
        if self.featured_schema:
            accept_result = self.featured_schema.accept(
                flatten_model(
                    feature_instance
                )
            )
        else:
            accept_result = False
        return accept_result


def numpy_dtype_to_python(dtype):
    mapping = {
        np.dtype('float64'): float,
        np.dtype('float32'): float,
        np.dtype('int64'): int,
        np.dtype('int32'): int,
        np.dtype('bool'): bool,
        # Add other dtypes as needed
    }
    return mapping.get(dtype, None)


class FeatureSchemaHandler:

    def __init__(self, service_config: ServiceLevelConfig):
        self.service_config = service_config

    def write(self, path: str, feature_schema_df: pd.DataFrame):
        feature_dtypes = feature_schema_df.iloc[:20].dtypes.to_dict()
        feature_types = dict([(k, v.name) for k, v in feature_dtypes.items()])

        file_service = file_services(self.service_config)
        schema_contents = orjson.dumps(feature_types).decode()
        file_service.write(path=path, contents=schema_contents)

    def read(self, path: str) -> dict:
        file_service = file_services(self.service_config)
        contents = file_service.load(path)
        schema_dict = orjson.loads(contents)
        return schema_dict

    def read_schema_class(self, path: str, model_class_name: str, model_package: str) -> ModelMetaclass:
        feature_schema = self.read(path)
        feature_schema_cls = create_model(model_class_name, __module__=model_package, **dict([(k, (numpy_dtype_to_python(np.dtype(v)), '')) for k, v in feature_schema.items()]))
        return feature_schema_cls


def flatten_model(model: BaseModel) -> Dict[str, Any]:
    output = {}
    
    for name, value in model.dict().items():
        if isinstance(value, dict):
            flattened = flatten_model(model.__fields__[name].outer_type_(**value))
            for k, v in flattened.items():
                output[f"{name}.{k}"] = v
        else:
            output[name] = value
            
    return output
