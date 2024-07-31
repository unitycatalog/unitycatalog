# FunctionInfo


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | Name of function, relative to parent schema. | [optional] 
**catalog_name** | **str** | Name of parent catalog. | [optional] 
**schema_name** | **str** | Name of parent schema relative to its parent catalog. | [optional] 
**input_params** | [**FunctionParameterInfos**](FunctionParameterInfos.md) |  | [optional] 
**data_type** | [**ColumnTypeName**](ColumnTypeName.md) |  | [optional] 
**full_data_type** | **str** | Pretty printed function data type. | [optional] 
**return_params** | [**FunctionParameterInfos**](FunctionParameterInfos.md) |  | [optional] 
**routine_body** | **str** | Function language. When **EXTERNAL** is used, the language of the routine function should be specified in the __external_language__ field,  and the __return_params__ of the function cannot be used (as **TABLE** return type is not supported), and the __sql_data_access__ field must be **NO_SQL**.  | [optional] 
**routine_definition** | **str** | Function body. | [optional] 
**routine_dependencies** | [**DependencyList**](DependencyList.md) |  | [optional] 
**parameter_style** | **str** | Function parameter style. **S** is the value for SQL. | [optional] 
**is_deterministic** | **bool** | Whether the function is deterministic. | [optional] 
**sql_data_access** | **str** | Function SQL data access. | [optional] 
**is_null_call** | **bool** | Function null call. | [optional] 
**security_type** | **str** | Function security type. | [optional] 
**specific_name** | **str** | Specific name of the function; Reserved for future use. | [optional] 
**comment** | **str** | User-provided free-form text description. | [optional] 
**properties** | **str** | JSON-serialized key-value pair map, encoded (escaped) as a string. | [optional] 
**full_name** | **str** | Full name of function, in form of __catalog_name__.__schema_name__.__function__name__ | [optional] 
**created_at** | **int** | Time at which this function was created, in epoch milliseconds. | [optional] 
**updated_at** | **int** | Time at which this function was last updated, in epoch milliseconds. | [optional] 
**function_id** | **str** | Id of Function, relative to parent schema. | [optional] 
**external_language** | **str** | External language of the function. | [optional] 

## Example

```python
from unitycatalog.models.function_info import FunctionInfo

# TODO update the JSON string below
json = "{}"
# create an instance of FunctionInfo from a JSON string
function_info_instance = FunctionInfo.from_json(json)
# print the JSON string representation of the object
print(FunctionInfo.to_json())

# convert the object into a dict
function_info_dict = function_info_instance.to_dict()
# create an instance of FunctionInfo from a dict
function_info_from_dict = FunctionInfo.from_dict(function_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


