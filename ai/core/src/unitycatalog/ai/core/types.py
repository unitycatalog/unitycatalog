import json
from typing import Any


class Variant:
    """
    A class to represent a variant type in Unity Catalog.
    """

    @classmethod
    def validate(cls, value: Any) -> None:
        """
        Validate the value against the variant type. This class method is a helper method
        to validate input data that you intend to pass as a VARIANT type in Unity Catalog.
        """
        if isinstance(value, (int, float, str, bool, type(None))):
            return
        elif isinstance(value, list):
            for item in value:
                cls.validate(item)
        elif isinstance(value, dict):
            for key, item in value.items():
                if not isinstance(key, str):
                    raise ValueError("VARIANT dictionary keys must be strings.")
                cls.validate(item)
        elif hasattr(value, "__dict__"):
            return
        else:
            raise ValueError(f"Unsupported type for VARIANT: {type(value)}")

    @classmethod
    def to_serializable(cls, value: Any) -> Any:
        """
        Convert the provided value to a JSON-serializable form.
        If the value is already serializable, return it directly.
        Otherwise, try to use its __dict__ or fall back to its string representation.
        """
        try:
            json.dumps(value)
            return value
        except TypeError:
            if hasattr(value, "__dict__"):
                return value.__dict__
            return str(value)
