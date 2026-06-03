# SchemaInfo
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | optional | Name of schema, relative to parent catalog. | |
| **catalog\_name** | **String** | optional | Name of parent catalog. | |
| **comment** | **String** | optional | User-provided free-form text description. | |
| **properties** | **Map** | optional | A map of key-value properties attached to the securable. | |
| **full\_name** | **String** | optional | Full name of schema, in form of __catalog_name__.__schema_name__. | |
| **owner** | **String** | optional | Username of current owner of schema. | |
| **created\_at** | **Long** | optional | Time at which this schema was created, in epoch milliseconds. | |
| **created\_by** | **String** | optional | Username of schema creator. | |
| **updated\_at** | **Long** | optional | Time at which this schema was last modified, in epoch milliseconds. | |
| **updated\_by** | **String** | optional | Username of user who last modified schema. | |
| **schema\_id** | **String** | optional | Unique identifier for the schema. | |
| **storage\_root** | **String** | optional | Storage root URL for managed storage location of schema. This can be set when creating a schema. Example: s3://bucket/ucroot  | |
| **storage\_location** | **String** | optional | Storage Location URL (full path) for managed storage location of schema. This is an automatically generated unique path under storage_root. If it is absent, managed securables under this schema will try to use storage_location of the parent catalog instead. Example: s3://bucket/ucroot/__unitystorage/schemas/{schema_id}  | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

