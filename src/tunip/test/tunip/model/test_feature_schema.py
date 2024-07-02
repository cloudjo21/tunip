import json
import pandas as pd
import pytest

from pydantic import ValidationError

from tunip.file_utils import services as file_services
from tunip.model.feature_schema import (
    FeatureSchemaHandler,
)
from tunip.service_config import get_service_config


@pytest.fixture(scope="module")
def model_input_feature_dataframe():
    return pd.DataFrame(
        {
            "A": [1, 2, 3],
            "B": [1.1, 2.2, 3.3]
        }
    )


def test_it_should_write_and_read_schema(model_input_feature_dataframe):
    feature_df = model_input_feature_dataframe
    service_config = get_service_config()
    feature_schema_handler = FeatureSchemaHandler(service_config)

    path = f"/user/{service_config.username}/lake/test/feature_schema"

    file_service = file_services(service_config)
    file_service.mkdirs(path)
    feature_schema_handler.write(f"{path}/schema.json", feature_df)
    schema_cls = feature_schema_handler.read_schema_class(f"{path}/schema.json", model_class_name="tunip.ModelInputFeatureSchema", model_package="tunip")
    schema_obj = schema_cls(A=1, B=1.1, C=11)
    assert schema_obj is not None
    assert "A" in schema_obj.model_json_schema().get("properties")
    assert "B" in schema_obj.model_json_schema().get("properties")
    assert "C" not in schema_obj.model_json_schema().get("properties")


def test_it_should_initialize_schema_using_dictionary_from_read_schema(model_input_feature_dataframe):
    feature_df = model_input_feature_dataframe
    service_config = get_service_config()
    feature_schema_handler = FeatureSchemaHandler(service_config)

    path = f"/user/{service_config.username}/lake/test/feature_schema"

    file_service = file_services(service_config)
    file_service.mkdirs(path)
    feature_schema_handler.write(f"{path}/schema.json", feature_df)
    schema_cls = feature_schema_handler.read_schema_class(f"{path}/schema.json", model_class_name="tunip.ModelInputFeatureSchema", model_package="tunip")
    data_json = {
        "A": 1,
        "B": 1.1,
        "C": 11
    }
    schema_obj = schema_cls(**data_json)
    assert schema_obj is not None
    assert "A" in schema_obj.model_json_schema().get("properties")
    assert "B" in schema_obj.model_json_schema().get("properties")
    assert "C" not in schema_obj.model_json_schema().get("properties")

    # use model_validate instead of parse_obj(deprecated)
    schema_obj2 = schema_cls.model_validate(data_json)
    # schema_obj2 = schema_cls.parse_obj(data_json)

    assert schema_obj2 is not None
    assert schema_obj2.A == 1

    with pytest.raises(ValidationError):
        schema_cls.model_validate({"A": 1.1, "B": 3})

    with pytest.raises(ValidationError):
        schema_cls.model_validate({"A": 1, "B": ""})

    schema_obj3 = schema_cls.model_validate_json(json.dumps({"A": 1, "B": 2.2}))
    assert schema_obj3 is not None

    with pytest.raises(ValidationError):
        schema_obj3 = schema_cls.model_validate(json.dumps({"A": 1}), strict=True)
        assert schema_obj3 is not None
