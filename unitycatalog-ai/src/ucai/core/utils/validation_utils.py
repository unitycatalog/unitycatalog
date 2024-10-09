import base64
import datetime
from typing import Any, NamedTuple

from ucai.core.utils.type_utils import is_time_type


class FullFunctionName(NamedTuple):
    catalog_name: str
    schema_name: str
    function_name: str


def validate_full_function_name(function_name: str) -> FullFunctionName:
    """
    Validate the full function name follows the format <catalog_name>.<schema_name>.<function_name>.

    Args:
        function_name (str): The full function name.

    Returns:
        FullFunctionName: The parsed full function name.
    """
    splits = function_name.split(".")
    if len(splits) != 3:
        raise ValueError(
            f"Invalid function name: {function_name}, expecting format <catalog_name>.<schema_name>.<function_name>."
        )
    return FullFunctionName(catalog_name=splits[0], schema_name=splits[1], function_name=splits[2])


def is_base64_encoded(s: str) -> bool:
    try:
        base64.b64decode(s, validate=True)
        return True
    except (base64.binascii.Error, ValueError):
        return False


def validate_param(param: Any, column_type: str, param_type_text: str) -> None:
    """
    Validate the parameter against the parameter info.

    Args:
        param (Any): The parameter to validate.
        column_type (str): The column type name.
        param_type_text (str): The parameter type text.
    """
    if is_time_type(column_type) and isinstance(param, str):
        try:
            datetime.datetime.fromisoformat(param)
        except ValueError as e:
            raise ValueError(f"Invalid datetime string: {param}, expecting ISO format.") from e
    elif column_type == "INTERVAL":
        # only day-time interval is supported, no year-month interval
        if isinstance(param, datetime.timedelta) and param_type_text != "interval day to second":
            raise ValueError(
                f"Invalid interval type text: {param_type_text}, expecting 'interval day to second', "
                "python timedelta can only be used for day-time interval."
            )
        # Only DAY TO SECOND is supported in python udf
        # rely on the SQL function for checking the interval format
        elif isinstance(param, str) and not (
            param.startswith("INTERVAL") and param.endswith("DAY TO SECOND")
        ):
            raise ValueError(
                f"Invalid interval string: {param}, expecting format `INTERVAL '[+|-] d[...] [h]h:[m]m:[s]s.ms[ms][ms][us][us][us]' DAY TO SECOND`."
            )
    elif column_type == "BINARY" and isinstance(param, str) and not is_base64_encoded(param):
        # the string value for BINARY column must be base64 encoded
        raise ValueError(
            f"The string input for column type BINARY must be base64 encoded, invalid input: {param}."
        )
