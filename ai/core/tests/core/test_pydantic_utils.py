from pydantic import BaseModel, Field, ValidationError

from unitycatalog.ai.core.utils.pydantic_utils import convert_pydantic_exception_to_readable


def test_friendly_validation_error_null_json():
    class JsonModel(BaseModel):
        json_field: dict

    try:
        JsonModel(json_field="NULL")
    except ValidationError as validation_err:
        friendly_msg = convert_pydantic_exception_to_readable(validation_err)

        assert "json_field" in friendly_msg
        assert "Could not parse value" in friendly_msg

        error_dict = validation_err.errors()[0]
        if error_dict.get("input_value") == "NULL":
            assert "SQL NULL values should be handled as Python None" in friendly_msg


def test_friendly_validation_error_missing_field():
    class RequiredFieldModel(BaseModel):
        required_field: str

    try:
        RequiredFieldModel()  # Missing required field
    except ValidationError as validation_err:
        friendly_msg = convert_pydantic_exception_to_readable(validation_err)

        assert "Missing required field" in friendly_msg


def test_friendly_validation_error_type_error():
    class TypeModel(BaseModel):
        integer_field: int

    try:
        TypeModel(integer_field="not an integer")
    except ValidationError as validation_err:
        friendly_msg = convert_pydantic_exception_to_readable(validation_err)

        assert "Field 'integer_field' has incorrect type" in friendly_msg


def test_friendly_validation_error_general():
    class ConstrainedModel(BaseModel):
        positive_number: int = Field(gt=0)

    try:
        ConstrainedModel(positive_number=-5)
    except ValidationError as validation_err:
        friendly_msg = convert_pydantic_exception_to_readable(validation_err)

        assert "positive_number" in friendly_msg
