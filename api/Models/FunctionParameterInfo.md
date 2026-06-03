# FunctionParameterInfo
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | required | Name of parameter. | |
| **type\_text** | **String** | required | Full data type spec, SQL/catalogString text. | |
| **type\_json** | **String** | required | Full data type spec, JSON-serialized. | |
| **type\_name** | [**ColumnTypeName**](ColumnTypeName.md) | required |  | |
| **type\_precision** | **Integer** | optional | Digits of precision; required on Create for DecimalTypes. | |
| **type\_scale** | **Integer** | optional | Digits to right of decimal; Required on Create for DecimalTypes. | |
| **type\_interval\_type** | **String** | optional | Format of IntervalType. | |
| **position** | **Integer** | required | Ordinal position of column (starting at position 0). | |
| **parameter\_mode** | [**FunctionParameterMode**](FunctionParameterMode.md) | optional |  | |
| **parameter\_type** | [**FunctionParameterType**](FunctionParameterType.md) | optional |  | |
| **parameter\_default** | **String** | optional | Default value of the parameter. | |
| **comment** | **String** | optional | User-provided free-form text description. | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

