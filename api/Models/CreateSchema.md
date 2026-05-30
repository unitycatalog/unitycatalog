# CreateSchema
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | required | Name of schema, relative to parent catalog. | |
| **catalog\_name** | **String** | required | Name of parent catalog. | |
| **comment** | **String** | optional | User-provided free-form text description. | |
| **properties** | **Map** | optional | A map of key-value properties attached to the securable. | |
| **storage\_root** | **String** | optional | Storage root URL for managed storage location of schema. If not set, managed securables under this schema will try to use the storage_location of the parent catalog instead. Example: s3://bucket/ucroot  | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

