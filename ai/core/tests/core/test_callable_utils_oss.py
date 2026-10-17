import decimal
import json

import pytest
from pydantic import ValidationError, create_model

from unitycatalog.ai.core.utils.callable_utils_oss import generate_function_info
from unitycatalog.ai.core.utils.function_processing_utils import param_info_to_pydantic_type


def collection_func(
    xs: list[int],
    weights: dict[str, list[float]],
    rates: dict[decimal.Decimal, int],
    label: str = "total",
) -> float:
    """
    A function with collection parameters.

    Args:
        xs: Numbers.
        weights: Weights per key.
        rates: Rates per amount.
        label: A label.
    """
    return 0.0


def test_generate_function_info_type_json_keeps_element_types():
    parameters = generate_function_info(collection_func).parameters

    assert [json.loads(p.type_json)["type"] for p in parameters] == [
        {"type": "array", "elementType": "long", "containsNull": True},
        {
            "type": "map",
            "keyType": "string",
            "valueType": {"type": "array", "elementType": "double", "containsNull": True},
            "valueContainsNull": True,
        },
        {
            "type": "map",
            "keyType": "decimal(38, 18)",
            "valueType": "long",
            "valueContainsNull": True,
        },
        "string",
    ]


def test_generate_function_info_type_json_round_trips_to_typed_schema():
    xs, weights, *_ = generate_function_info(collection_func).parameters
    model = create_model(
        "CollectionFuncParams",
        xs=(param_info_to_pydantic_type(xs).pydantic_type, ...),
        weights=(param_info_to_pydantic_type(weights).pydantic_type, ...),
    )

    model(xs=[1, 2], weights={"a": [0.5]})
    with pytest.raises(ValidationError):
        model(xs=["a"], weights={"a": [0.5]})
    with pytest.raises(ValidationError):
        model(xs=[1, 2], weights={"a": "x"})
