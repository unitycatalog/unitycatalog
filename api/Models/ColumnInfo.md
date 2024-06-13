# ColumnInfo
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | Name of Column. | [optional] [default to null] |
| **type\_text** | **String** | Full data type specification as SQL/catalogString text. | [optional] [default to null] |
| **type\_json** | **String** | Full data type specification, JSON-serialized. | [optional] [default to null] |
| **type\_name** | [**ColumnTypeName**](ColumnTypeName.md) |  | [optional] [default to null] |
| **type\_precision** | **Integer** | Digits of precision; required for DecimalTypes. | [optional] [default to null] |
| **type\_scale** | **Integer** | Digits to right of decimal; Required for DecimalTypes. | [optional] [default to null] |
| **type\_interval\_type** | **String** | Format of IntervalType. | [optional] [default to null] |
| **position** | **Integer** | Ordinal position of column (starting at position 0). | [optional] [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |
| **nullable** | **Boolean** | Whether field may be Null. | [optional] [default to true] |
| **partition\_index** | **Integer** | Partition index for column. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

