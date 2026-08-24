# CreateFunction
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | Name of function, relative to parent schema. | [default to null] |
| **catalog\_name** | **String** | Name of parent catalog. | [default to null] |
| **schema\_name** | **String** | Name of parent schema relative to its parent catalog. | [default to null] |
| **input\_params** | [**FunctionParameterInfos**](FunctionParameterInfos.md) |  | [default to null] |
| **data\_type** | [**ColumnTypeName**](ColumnTypeName.md) |  | [default to null] |
| **full\_data\_type** | **String** | Pretty printed function data type. | [default to null] |
| **return\_params** | [**FunctionParameterInfos**](FunctionParameterInfos.md) |  | [optional] [default to null] |
| **routine\_body** | **String** | Function language. When **EXTERNAL** is used, the language of the routine function should be specified in the __external_language__ field,  and the __return_params__ of the function cannot be used (as **TABLE** return type is not supported), and the __sql_data_access__ field must be **NO_SQL**.  | [default to null] |
| **routine\_definition** | **String** | Function body. | [default to null] |
| **routine\_dependencies** | [**DependencyList**](DependencyList.md) |  | [optional] [default to null] |
| **parameter\_style** | **String** | Function parameter style. **S** is the value for SQL. | [default to null] |
| **is\_deterministic** | **Boolean** | Whether the function is deterministic. | [default to null] |
| **sql\_data\_access** | **String** | Function SQL data access. | [default to null] |
| **is\_null\_call** | **Boolean** | Function null call. | [default to null] |
| **security\_type** | **String** | Function security type. | [default to null] |
| **specific\_name** | **String** | Specific name of the function; Reserved for future use. | [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |
| **properties** | **String** | JSON-serialized key-value pair map, encoded (escaped) as a string. | [optional] [default to null] |
| **external\_language** | **String** | External language of the function. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

