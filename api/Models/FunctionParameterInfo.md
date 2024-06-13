# FunctionParameterInfo
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | Name of parameter. | [default to null] |
| **type\_text** | **String** | Full data type spec, SQL/catalogString text. | [default to null] |
| **type\_json** | **String** | Full data type spec, JSON-serialized. | [default to null] |
| **type\_name** | [**ColumnTypeName**](ColumnTypeName.md) |  | [default to null] |
| **type\_precision** | **Integer** | Digits of precision; required on Create for DecimalTypes. | [optional] [default to null] |
| **type\_scale** | **Integer** | Digits to right of decimal; Required on Create for DecimalTypes. | [optional] [default to null] |
| **type\_interval\_type** | **String** | Format of IntervalType. | [optional] [default to null] |
| **position** | **Integer** | Ordinal position of column (starting at position 0). | [default to null] |
| **parameter\_mode** | [**FunctionParameterMode**](FunctionParameterMode.md) |  | [optional] [default to null] |
| **parameter\_type** | [**FunctionParameterType**](FunctionParameterType.md) |  | [optional] [default to null] |
| **parameter\_default** | **String** | Default value of the parameter. | [optional] [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

