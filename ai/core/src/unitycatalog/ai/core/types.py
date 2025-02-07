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
        else:
            raise ValueError(f"Unsupported type for VARIANT: {type(value)}")
