# CreateTable
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | Name of table, relative to parent schema. | [default to null] |
| **catalog\_name** | **String** | Name of parent catalog. | [default to null] |
| **schema\_name** | **String** | Name of parent schema relative to its parent catalog. | [default to null] |
| **table\_type** | [**TableType**](TableType.md) |  | [default to null] |
| **data\_source\_format** | [**DataSourceFormat**](DataSourceFormat.md) |  | [default to null] |
| **columns** | [**List**](ColumnInfo.md) | The array of __ColumnInfo__ definitions of the table&#39;s columns. | [default to null] |
| **storage\_location** | **String** | Storage root URL for external table | [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |
| **properties** | **Map** | A map of key-value properties attached to the securable. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

