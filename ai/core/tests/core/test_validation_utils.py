import datetime
import warnings

import pytest
from databricks.sdk.service.catalog import (
    ColumnTypeName,
    CreateFunctionParameterStyle,
    CreateFunctionRoutineBody,
    CreateFunctionSecurityType,
    CreateFunctionSqlDataAccess,
    DependencyList,
    FunctionInfo,
    FunctionParameterInfo,
    FunctionParameterInfos,
)
from pydantic_core import to_jsonable_python

from unitycatalog.ai.core.utils.validation_utils import (
    check_function_info,
    has_retriever_signature,
    parse_iso_datetime,
    validate_param,
)


@pytest.fixture
def function_info(request):
    """
    Parametrized fixture to create FunctionInfo objects with various configurations.

    Parameters passed via request.param:
        - missing_param_comments: list of parameter names to omit comments
        - missing_function_comment: bool indicating whether to omit the function comment
    """
    missing_param_comments = request.param.get("missing_param_comments", [])
    missing_function_comment = request.param.get("missing_function_comment", False)

    func_name = "test_func"

    parameters = [
        FunctionParameterInfo(
            name="a",
            type_name=ColumnTypeName.INT,
            type_text="int",
            position=0,
            comment=None if "a" in missing_param_comments else "Parameter a description",
        ),
        FunctionParameterInfo(
            name="b",
            type_name=ColumnTypeName.STRING,
            type_text="string",
            position=1,
            comment=None if "b" in missing_param_comments else "Parameter b description",
        ),
    ]

    func_comment = None if missing_function_comment else "Function description"

    return FunctionInfo(
        catalog_name="CATALOG",
        schema_name="SCHEMA",
        name=func_name,
        input_params=FunctionParameterInfos(parameters=parameters),
        data_type=ColumnTypeName.STRING,
        external_language="Python",
        comment=func_comment,
        routine_body=CreateFunctionRoutineBody.EXTERNAL,
        routine_definition="return str(a) + b",
        full_data_type="STRING",
        return_params=FunctionParameterInfos(),
        routine_dependencies=DependencyList(),
        parameter_style=CreateFunctionParameterStyle.S,
        is_deterministic=False,
        sql_data_access=CreateFunctionSqlDataAccess.NO_SQL,
        is_null_call=False,
        security_type=CreateFunctionSecurityType.DEFINER,
        specific_name=func_name,
    )


@pytest.mark.parametrize(
    "function_info, expected_warnings",
    [
        ({"missing_param_comments": [], "missing_function_comment": False}, []),
        (
            {"missing_param_comments": [], "missing_function_comment": True},
            ["The function test_func does not have a description"],
        ),
        (
            {"missing_param_comments": ["a"], "missing_function_comment": False},
            ["The following parameters do not have descriptions: a for the function"],
        ),
        (
            {"missing_param_comments": ["b"], "missing_function_comment": False},
            ["The following parameters do not have descriptions: b for the function"],
        ),
        (
            {"missing_param_comments": ["a", "b"], "missing_function_comment": True},
            [
                "The following parameters do not have descriptions: a, b for the function",
                "The function test_func does not have a description",
            ],
        ),
    ],
    indirect=["function_info"],
)
def test_check_function_info(function_info, expected_warnings):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        check_function_info(function_info)

        emitted_warnings = [
            str(warn.message) for warn in w if issubclass(warn.category, UserWarning)
        ]

    for expected_warning in expected_warnings:
        assert any(expected_warning in msg for msg in emitted_warnings)

    if expected_warnings:
        assert len(emitted_warnings) == len(expected_warnings)
    else:
        assert len(emitted_warnings) == 0, f"Unexpected warnings emitted: {emitted_warnings}"


@pytest.mark.parametrize(
    "function_info, result",
    [
        (FunctionInfo(data_type=ColumnTypeName.STRING, full_data_type="STRING"), False),
        (
            FunctionInfo(
                data_type=ColumnTypeName.STRING,
                full_data_type="(page_content STRING)",
                return_params=None,
            ),
            False,
        ),
        (
            FunctionInfo(
                data_type=ColumnTypeName.TABLE_TYPE,
                full_data_type="(page_content STRING)",
                return_params=FunctionParameterInfos(
                    parameters=[
                        FunctionParameterInfo(
                            name="page_content",
                            type_text="string",
                            type_name=ColumnTypeName.STRING,
                            position=0,
                        )
                    ]
                ),
            ),
            True,
        ),
        (
            FunctionInfo(
                data_type=ColumnTypeName.TABLE_TYPE,
                full_data_type="(page_content STRING, metadata MAP<STRING, STRING>, id STRING)",
                return_params=FunctionParameterInfos(
                    parameters=[
                        FunctionParameterInfo(
                            name="page_content",
                            type_text="string",
                            type_name=ColumnTypeName.STRING,
                            position=0,
                        ),
                        FunctionParameterInfo(
                            name="metadata",
                            type_text="map<string,string>",
                            type_name=ColumnTypeName.MAP,
                            position=1,
                        ),
                        FunctionParameterInfo(
                            name="id",
                            type_text="string",
                            type_name=ColumnTypeName.STRING,
                            position=2,
                        ),
                    ]
                ),
            ),
            True,
        ),
        (
            FunctionInfo(
                data_type=ColumnTypeName.TABLE_TYPE,
                full_data_type="(metadata MAP<STRING, STRING>, id STRING)",
                return_params=FunctionParameterInfos(
                    parameters=[
                        FunctionParameterInfo(
                            name="metadata",
                            type_text="map<string,string>",
                            type_name=ColumnTypeName.MAP,
                            position=0,
                        ),
                        FunctionParameterInfo(
                            name="id",
                            type_text="string",
                            type_name=ColumnTypeName.STRING,
                            position=1,
                        ),
                    ]
                ),
            ),
            False,
        ),
        (
            FunctionInfo(
                data_type=ColumnTypeName.TABLE_TYPE,
                full_data_type="(page_content STRING, metadata MAP<STRING, STRING>, extra_column STRING)",
                return_params=FunctionParameterInfos(
                    parameters=[
                        FunctionParameterInfo(
                            name="page_content",
                            type_text="string",
                            type_name=ColumnTypeName.STRING,
                            position=0,
                        ),
                        FunctionParameterInfo(
                            name="metadata",
                            type_text="map<string,string>",
                            type_name=ColumnTypeName.MAP,
                            position=1,
                        ),
                        FunctionParameterInfo(
                            name="extra_column",
                            type_text="string",
                            type_name=ColumnTypeName.STRING,
                            position=2,
                        ),
                    ]
                ),
            ),
            True,
        ),
    ],
)
def test_has_retriever_signature(function_info, result):
    assert has_retriever_signature(function_info) == result


def test_validate_param_accepts_utc_timestamp_sent_by_tool_integrations():
    at = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    value = to_jsonable_python(at)

    assert value == "2024-01-02T03:04:05Z"
    validate_param(value, "TIMESTAMP", "timestamp")
    assert parse_iso_datetime(value) == at


@pytest.mark.parametrize("value", ["2024-01-02T03:04:05+00:00", "2024-01-02T03:04:05"])
def test_parse_iso_datetime_keeps_other_forms(value):
    assert parse_iso_datetime(value) == datetime.datetime.fromisoformat(value)


def test_validate_param_rejects_non_iso_timestamp():
    with pytest.raises(ValueError, match="expecting ISO format"):
        validate_param("2024-01-02 03:04:05 UTC", "TIMESTAMP", "timestamp")
