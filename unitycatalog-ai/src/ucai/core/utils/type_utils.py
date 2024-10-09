import datetime
import decimal
from typing import Any

SQL_TYPE_TO_PYTHON_TYPE_MAPPING = {
    # numpy array is not accepted, it's not json serializable
    "ARRAY": (list, tuple),
    "BINARY": (bytes, str),
    "BOOLEAN": bool,
    # tinyint type
    "BYTE": int,
    "CHAR": str,
    "DATE": (datetime.date, str),
    # no precision and scale check, rely on SQL function to validate
    "DECIMAL": (decimal.Decimal, float),
    "DOUBLE": float,
    "FLOAT": float,
    "INT": int,
    "INTERVAL": (datetime.timedelta, str),
    "LONG": int,
    "MAP": dict,
    # ref: https://docs.databricks.com/en/error-messages/datatype-mismatch-error-class.html#null_type
    # it's not supported in return data type as well `[UNSUPPORTED_DATATYPE] Unsupported data type "NULL". SQLSTATE: 0A000`
    "NULL": type(None),
    "SHORT": int,
    "STRING": str,
    "STRUCT": dict,
    # not allowed for python udf, users should only pass string
    "TABLE_TYPE": str,
    "TIMESTAMP": (datetime.datetime, str),
    "TIMESTAMP_NTZ": (datetime.datetime, str),
    # it's a type that can be defined in scala, python shouldn't force check the type here
    # ref: https://www.waitingforcode.com/apache-spark-sql/used-defined-type/read
    "USER_DEFINED_TYPE": object,
}

UC_TYPE_JSON_MAPPING = {
    **SQL_TYPE_TO_PYTHON_TYPE_MAPPING,
    "INTEGER": int,
    # The binary field should be a string expression in base64 format
    "BINARY": bytes,
    "INTERVAL DAY TO SECOND": (datetime.timedelta, str),
}


def column_type_to_python_type(column_type: str) -> Any:
    """
    Convert a SQL column type to the corresponding Python type.
    Looks up the provided SQL column type in a mapping dictionary and returns
    the corresponding Python type. Raises a `ValueError` if the column type
    is unsupported.
    Args:
        column_type (str): The SQL column type to convert.
    Returns:
        Any: The corresponding Python type.
    Raises:
        ValueError: If the column type is unsupported.
    """
    if t := SQL_TYPE_TO_PYTHON_TYPE_MAPPING.get(column_type):
        return t
    raise ValueError(f"Unsupported column type: {column_type}")


def is_time_type(column_type: str) -> bool:
    """
    Check if the column type is a time-related type.
    Determines if the given SQL column type represents a date or timestamp type.
    Args:
        column_type (str): The SQL column type to check.
    Returns:
        bool: True if the column type is time-related, False otherwise.
    """
    return column_type in (
        "DATE",
        "TIMESTAMP",
        "TIMESTAMP_NTZ",
    )


def convert_timedelta_to_interval_str(time_val: datetime.timedelta) -> str:
    """
    Convert a timedelta object to a string representing an interval in the format of 'INTERVAL "d hh:mm:ss.ssssss"'.
    """
    days = time_val.days
    hours, remainder = divmod(time_val.seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    microseconds = time_val.microseconds
    return f"INTERVAL '{days} {hours}:{minutes}:{seconds}.{microseconds}' DAY TO SECOND"
