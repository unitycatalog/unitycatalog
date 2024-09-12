# TableInfo
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | Name of table, relative to parent schema. | [optional] [default to null] |
| **catalog\_name** | **String** | Name of parent catalog. | [optional] [default to null] |
| **schema\_name** | **String** | Name of parent schema relative to its parent catalog. | [optional] [default to null] |
| **table\_type** | [**TableType**](TableType.md) |  | [optional] [default to null] |
| **data\_source\_format** | [**DataSourceFormat**](DataSourceFormat.md) |  | [optional] [default to null] |
| **columns** | [**List**](ColumnInfo.md) | The array of __ColumnInfo__ definitions of the table&#39;s columns. | [optional] [default to null] |
| **storage\_location** | **String** | Storage root URL for table (for **MANAGED**, **EXTERNAL** tables) | [optional] [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |
| **properties** | **Map** | A map of key-value properties attached to the securable. | [optional] [default to null] |
| **owner** | **String** | Username of current owner of table. | [optional] [default to null] |
| **created\_at** | **Long** | Time at which this table was created, in epoch milliseconds. | [optional] [default to null] |
| **created\_by** | **String** | Username of table creator. | [optional] [default to null] |
| **updated\_at** | **Long** | Time at which this table was last modified, in epoch milliseconds. | [optional] [default to null] |
| **updated\_by** | **String** | Username of user who last modified the table. | [optional] [default to null] |
| **table\_id** | **String** | Unique identifier for the table. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

