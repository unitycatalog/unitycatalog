import functools
from dataclasses import dataclass
from typing import Any, Optional, Type, Union

from pydantic import BaseModel, ValidationError


@dataclass
class PydanticType:
    pydantic_type: Type
    strict: bool = False


@dataclass
class PydanticField:
    pydantic_type: Type
    description: Optional[str] = None
    default: Optional[Any] = None
    strict: bool = False


@dataclass
class PydanticFunctionInputParams:
    pydantic_model: Type[BaseModel]
    strict: bool = False


def convert_pydantic_exception_to_readable(
    validation_error: Union[ValidationError, Exception],
) -> str:
    """
    Convert Pydantic validation errors into more user-friendly error messages.

    Args:
        validation_error: The validation error from Pydantic

    Returns:
        A user-friendly error message
    """
    if isinstance(validation_error, ValidationError):
        errors = validation_error.errors()
        friendly_messages = []
        error_str = str(validation_error)

        for error in errors:
            field_path = " -> ".join(str(loc) for loc in error["loc"])
            error_type = error.get("type", "")
            msg = error.get("msg", "")

            input_value = error.get("input_value", "")
            ctx = error.get("ctx", {})
            if ctx and isinstance(ctx, dict):
                input_value = ctx.get("input_value", input_value)

            is_null = (
                (isinstance(input_value, str) and input_value.upper() == "NULL")
                or "NULL" in error_str.upper()
                and field_path in error_str
            )

            if is_null:
                friendly_messages.append(
                    f"Could not parse value for '{field_path}'. SQL NULL values should be handled as Python None."
                )
            elif "dict" in error_type.lower() or "type_error.dict" in error_type:
                friendly_messages.append(
                    f"Could not parse value for '{field_path}'. Input should be a valid dictionary."
                )
            elif "missing" in error_type.lower():
                friendly_messages.append(f"Missing required field: '{field_path}'")
            elif "int_parsing" in error_type or "type" in error_type.lower():
                friendly_messages.append(f"Field '{field_path}' has incorrect type. {msg}")
            else:
                friendly_messages.append(f"Error in field '{field_path}': {msg}")

        if friendly_messages:
            return "Validation errors:\n- " + "\n- ".join(friendly_messages)
        return str(validation_error)

    return str(validation_error)


def handle_pydantic_validation_errors(func):
    """
    Decorator to handle Pydantic validation errors with friendly messages.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ValidationError as e:
            friendly_error = convert_pydantic_exception_to_readable(e)
            raise ValueError(f"Validation failed: {friendly_error}") from e

    return wrapper
