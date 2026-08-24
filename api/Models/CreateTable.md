# CreateTable
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | Name of table, relative to parent schema. | [default to null] |
| **catalog\_name** | **String** | Name of parent catalog. | [default to null] |
| **schema\_name** | **String** | Name of parent schema relative to its parent catalog. | [default to null] |
| **table\_type** | [**TableType**](TableType.md) |  | [default to null] |
| **data\_source\_format** | [**DataSourceFormat**](DataSourceFormat.md) |  | [optional] [default to null] |
| **columns** | [**List**](ColumnInfo.md) | The array of __ColumnInfo__ definitions of the table&#39;s columns. | [default to null] |
| **storage\_location** | **String** | Storage root URL for external table | [optional] [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |
| **properties** | **Map** | A map of key-value properties attached to the securable. | [optional] [default to null] |
| **view\_definition** | **String** | Definition text for view-like table types such as VIEW, MATERIALIZED_VIEW, STREAMING_TABLE, and METRIC_VIEW. The format depends on the table type (SQL for views, YAML for metric views). | [optional] [default to null] |
| **view\_dependencies** | [**DependencyList**](DependencyList.md) |  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

