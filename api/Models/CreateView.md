# CreateView
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | Name of view, relative to parent schema. | [default to null] |
| **catalog\_name** | **String** | Name of parent catalog. | [default to null] |
| **schema\_name** | **String** | Name of parent schema relative to its parent catalog. | [default to null] |
| **columns** | [**List**](ColumnInfo.md) | The array of __ColumnInfo__ definitions of the view&#39;s columns. | [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |
| **properties** | **Map** | A map of key-value properties attached to the securable. | [optional] [default to null] |
| **view\_definition** | [**List**](ViewRepresentation.md) | View definition with SQL representations. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

