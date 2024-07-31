# CreateFunction


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | Name of function, relative to parent schema. | 
**catalog_name** | **str** | Name of parent catalog. | 
**schema_name** | **str** | Name of parent schema relative to its parent catalog. | 
**input_params** | [**FunctionParameterInfos**](FunctionParameterInfos.md) |  | 
**data_type** | [**ColumnTypeName**](ColumnTypeName.md) |  | 
**full_data_type** | **str** | Pretty printed function data type. | 
**return_params** | [**FunctionParameterInfos**](FunctionParameterInfos.md) |  | [optional] 
**routine_body** | **str** | Function language. When **EXTERNAL** is used, the language of the routine function should be specified in the __external_language__ field,  and the __return_params__ of the function cannot be used (as **TABLE** return type is not supported), and the __sql_data_access__ field must be **NO_SQL**.  | 
**routine_definition** | **str** | Function body. | 
**routine_dependencies** | [**DependencyList**](DependencyList.md) |  | [optional] 
**parameter_style** | **str** | Function parameter style. **S** is the value for SQL. | 
**is_deterministic** | **bool** | Whether the function is deterministic. | 
**sql_data_access** | **str** | Function SQL data access. | 
**is_null_call** | **bool** | Function null call. | 
**security_type** | **str** | Function security type. | 
**specific_name** | **str** | Specific name of the function; Reserved for future use. | 
**comment** | **str** | User-provided free-form text description. | [optional] 
**properties** | **str** | JSON-serialized key-value pair map, encoded (escaped) as a string. | 
**external_language** | **str** | External language of the function. | [optional] 

## Example

```python
from unitycatalog.models.create_function import CreateFunction

# TODO update the JSON string below
json = "{}"
# create an instance of CreateFunction from a JSON string
create_function_instance = CreateFunction.from_json(json)
# print the JSON string representation of the object
print(CreateFunction.to_json())

# convert the object into a dict
create_function_dict = create_function_instance.to_dict()
# create an instance of CreateFunction from a dict
create_function_from_dict = CreateFunction.from_dict(create_function_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


