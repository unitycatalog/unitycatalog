# ColumnInfo
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | optional | Name of Column. | |
| **type\_text** | **String** | optional | Full data type specification as SQL/catalogString text. | |
| **type\_json** | **String** | optional | Full data type specification, JSON-serialized. | |
| **type\_name** | [**ColumnTypeName**](ColumnTypeName.md) | optional |  | |
| **type\_precision** | **Integer** | optional | Digits of precision; required for DecimalTypes. | |
| **type\_scale** | **Integer** | optional | Digits to right of decimal; Required for DecimalTypes. | |
| **type\_interval\_type** | **String** | optional | Format of IntervalType. | |
| **position** | **Integer** | optional | Ordinal position of column (starting at position 0). | |
| **comment** | **String** | optional | User-provided free-form text description. | |
| **nullable** | **Boolean** | optional | Whether field may be Null. | [default to true] |
| **partition\_index** | **Integer** | optional | Partition index for column. | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

