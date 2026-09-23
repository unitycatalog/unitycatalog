# UpdateView
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **table\_type** | [**TableType**](TableType.md) |  | [default to null] |
| **columns** | [**List**](ColumnInfo.md) | The array of __ColumnInfo__ definitions exposed by the view. | [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |
| **properties** | **Map** | A map of key-value properties attached to the securable. | [optional] [default to null] |
| **view\_definition** | **String** | Definition text for the view. The format depends on the table type (SQL for views, YAML for metric views). | [default to null] |
| **view\_dependencies** | [**DependencyList**](DependencyList.md) |  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

