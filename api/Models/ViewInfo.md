# ViewInfo
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | Name of view, relative to parent schema. | [optional] [default to null] |
| **view\_id** | **String** | Unique identifier for the view. | [optional] [default to null] |
| **catalog\_name** | **String** | Name of parent catalog. | [optional] [default to null] |
| **schema\_name** | **String** | Name of parent schema relative to its parent catalog. | [optional] [default to null] |
| **columns** | [**List**](ColumnInfo.md) | List of column definitions for this schema version. | [optional] [default to null] |
| **representations** | [**List**](ViewRepresentation.md) | List of SQL representations for this view. | [optional] [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |
| **properties** | **Map** | A map of key-value properties attached to the securable. | [optional] [default to null] |
| **owner** | **String** | Username of current owner of view. | [optional] [default to null] |
| **created\_at** | **Long** | Time at which this view was created, in epoch milliseconds. | [optional] [default to null] |
| **created\_by** | **String** | Username of view creator. | [optional] [default to null] |
| **updated\_at** | **Long** | Time at which this view was last modified, in epoch milliseconds. | [optional] [default to null] |
| **updated\_by** | **String** | Username of user who last modified the view. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

