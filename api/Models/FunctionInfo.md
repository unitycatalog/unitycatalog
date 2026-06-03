# FunctionInfo
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | optional | Name of function, relative to parent schema. | |
| **catalog\_name** | **String** | optional | Name of parent catalog. | |
| **schema\_name** | **String** | optional | Name of parent schema relative to its parent catalog. | |
| **input\_params** | [**FunctionParameterInfos**](FunctionParameterInfos.md) | optional |  | |
| **data\_type** | [**ColumnTypeName**](ColumnTypeName.md) | optional |  | |
| **full\_data\_type** | **String** | optional | Pretty printed function data type. | |
| **return\_params** | [**FunctionParameterInfos**](FunctionParameterInfos.md) | optional |  | |
| **routine\_body** | **String** | optional | Function language. When **EXTERNAL** is used, the language of the routine function should be specified in the __external_language__ field,  and the __return_params__ of the function cannot be used (as **TABLE** return type is not supported), and the __sql_data_access__ field must be **NO_SQL**.  | |
| **routine\_definition** | **String** | optional | Function body. | |
| **routine\_dependencies** | [**DependencyList**](DependencyList.md) | optional |  | |
| **parameter\_style** | **String** | optional | Function parameter style. **S** is the value for SQL. | |
| **is\_deterministic** | **Boolean** | optional | Whether the function is deterministic. | |
| **sql\_data\_access** | **String** | optional | Function SQL data access. | |
| **is\_null\_call** | **Boolean** | optional | Function null call. | |
| **security\_type** | **String** | optional | Function security type. | |
| **specific\_name** | **String** | optional | Specific name of the function; Reserved for future use. | |
| **comment** | **String** | optional | User-provided free-form text description. | |
| **properties** | **String** | optional | JSON-serialized key-value pair map, encoded (escaped) as a string. | |
| **full\_name** | **String** | optional | Full name of function, in form of __catalog_name__.__schema_name__.__function__name__ | |
| **owner** | **String** | optional | Username of current owner of function. | |
| **created\_at** | **Long** | optional | Time at which this function was created, in epoch milliseconds. | |
| **created\_by** | **String** | optional | Username of function creator. | |
| **updated\_at** | **Long** | optional | Time at which this function was last updated, in epoch milliseconds. | |
| **updated\_by** | **String** | optional | Username of user who last modified function. | |
| **function\_id** | **String** | optional | Id of Function, relative to parent schema. | |
| **external\_language** | **String** | optional | External language of the function. | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

