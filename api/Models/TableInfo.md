# TableInfo
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | optional | Name of table, relative to parent schema. | |
| **catalog\_name** | **String** | optional | Name of parent catalog. | |
| **schema\_name** | **String** | optional | Name of parent schema relative to its parent catalog. | |
| **table\_type** | [**TableType**](TableType.md) | optional |  | |
| **data\_source\_format** | [**DataSourceFormat**](DataSourceFormat.md) | optional |  | |
| **columns** | [**List**](ColumnInfo.md) | optional | The array of __ColumnInfo__ definitions of the table&#39;s columns. | |
| **storage\_location** | **String** | optional | Storage root URL for table (for **MANAGED**, **EXTERNAL** tables) | |
| **comment** | **String** | optional | User-provided free-form text description. | |
| **properties** | **Map** | optional | A map of key-value properties attached to the securable. | |
| **owner** | **String** | optional | Username of current owner of table. | |
| **created\_at** | **Long** | optional | Time at which this table was created, in epoch milliseconds. | |
| **created\_by** | **String** | optional | Username of table creator. | |
| **updated\_at** | **Long** | optional | Time at which this table was last modified, in epoch milliseconds. | |
| **updated\_by** | **String** | optional | Username of user who last modified the table. | |
| **table\_id** | **String** | optional | Unique identifier for the table. | |
| **view\_definition** | **String** | optional | Definition text for view-like table types such as VIEW, MATERIALIZED_VIEW, STREAMING_TABLE, and METRIC_VIEW. The format depends on the table type (SQL for views, YAML for metric views). | |
| **view\_dependencies** | [**DependencyList**](DependencyList.md) | optional |  | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

