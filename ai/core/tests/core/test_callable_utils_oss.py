import datetime
import decimal
import json

import pytest
from pydantic import ValidationError, create_model

from unitycatalog.ai.core.utils.callable_utils_oss import generate_function_info
from unitycatalog.ai.core.utils.function_processing_utils import param_info_to_pydantic_type


def collection_func(
    xs: list[int],
    grid: list[list[int]],
    weights: dict[str, list[float]],
    rates: dict[decimal.Decimal, int],
    counts: dict[int, float],
    days: list[datetime.date],
    label: str = "total",
) -> float:
    """
    A function with collection parameters.

    Args:
        xs: Numbers.
        grid: Rows of numbers.
        weights: Weights per key.
        rates: Rates per amount.
        counts: Counts per id.
        days: Days.
        label: A label.
    """
    return 0.0


@pytest.fixture(scope="module")
def parameters():
    return {p.name: p for p in generate_function_info(collection_func).parameters}


def test_generate_function_info_type_json_keeps_element_types(parameters):
    assert {name: json.loads(p.type_json)["type"] for name, p in parameters.items()} == {
        "xs": {"type": "array", "elementType": "long", "containsNull": True},
        "grid": {
            "type": "array",
            "elementType": {"type": "array", "elementType": "long", "containsNull": True},
            "containsNull": True,
        },
        "weights": {
            "type": "map",
            "keyType": "string",
            "valueType": {"type": "array", "elementType": "double", "containsNull": True},
            "valueContainsNull": True,
        },
        "rates": {
            "type": "map",
            "keyType": "decimal(38, 18)",
            "valueType": "long",
            "valueContainsNull": True,
        },
        "counts": {
            "type": "map",
            "keyType": "long",
            "valueType": "double",
            "valueContainsNull": True,
        },
        "days": {"type": "array", "elementType": "date", "containsNull": True},
        "label": "string",
    }


def test_generate_function_info_type_json_round_trips_to_typed_schema(parameters):
    model = create_model(
        "CollectionFuncParams",
        **{
            name: (param_info_to_pydantic_type(parameters[name]).pydantic_type, ...)
            for name in ("xs", "grid", "weights")
        },
    )
    valid = {"xs": [1, 2], "grid": [[1], [2, 3]], "weights": {"a": [0.5]}}

    model(**valid)
    for invalid in ({"xs": ["a"]}, {"grid": [["a"]]}, {"weights": {"a": ["x"]}}):
        with pytest.raises(ValidationError):
            model(**{**valid, **invalid})


@pytest.mark.parametrize("name", ["rates", "counts"])
def test_generate_function_info_non_string_map_key_is_rejected(parameters, name):
    with pytest.raises(TypeError, match="Only support STRING key type for MAP"):
        param_info_to_pydantic_type(parameters[name])


@pytest.mark.parametrize(
    ("name", "strict"),
    [("xs", True), ("grid", True), ("weights", True), ("days", False)],
)
def test_generate_function_info_strictness_follows_element_type(parameters, name, strict):
    assert param_info_to_pydantic_type(parameters[name], strict=True).strict is strict
