# CreateFunction
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | required | Name of function, relative to parent schema. | |
| **catalog\_name** | **String** | required | Name of parent catalog. | |
| **schema\_name** | **String** | required | Name of parent schema relative to its parent catalog. | |
| **input\_params** | [**FunctionParameterInfos**](FunctionParameterInfos.md) | required |  | |
| **data\_type** | [**ColumnTypeName**](ColumnTypeName.md) | required |  | |
| **full\_data\_type** | **String** | required | Pretty printed function data type. | |
| **return\_params** | [**FunctionParameterInfos**](FunctionParameterInfos.md) | optional |  | |
| **routine\_body** | **String** | required | Function language. When **EXTERNAL** is used, the language of the routine function should be specified in the __external_language__ field,  and the __return_params__ of the function cannot be used (as **TABLE** return type is not supported), and the __sql_data_access__ field must be **NO_SQL**.  | |
| **routine\_definition** | **String** | required | Function body. | |
| **routine\_dependencies** | [**DependencyList**](DependencyList.md) | optional |  | |
| **parameter\_style** | **String** | required | Function parameter style. **S** is the value for SQL. | |
| **is\_deterministic** | **Boolean** | required | Whether the function is deterministic. | |
| **sql\_data\_access** | **String** | required | Function SQL data access. | |
| **is\_null\_call** | **Boolean** | required | Function null call. | |
| **security\_type** | **String** | required | Function security type. | |
| **specific\_name** | **String** | required | Specific name of the function; Reserved for future use. | |
| **comment** | **String** | optional | User-provided free-form text description. | |
| **properties** | **String** | optional | JSON-serialized key-value pair map, encoded (escaped) as a string. | |
| **external\_language** | **String** | optional | External language of the function. | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

