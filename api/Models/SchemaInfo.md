# SchemaInfo
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | Name of schema, relative to parent catalog. | [optional] [default to null] |
| **catalog\_name** | **String** | Name of parent catalog. | [optional] [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |
| **properties** | **Map** | A map of key-value properties attached to the securable. | [optional] [default to null] |
| **full\_name** | **String** | Full name of schema, in form of __catalog_name__.__schema_name__. | [optional] [default to null] |
| **owner** | **String** | Username of current owner of schema. | [optional] [default to null] |
| **created\_at** | **Long** | Time at which this schema was created, in epoch milliseconds. | [optional] [default to null] |
| **created\_by** | **String** | Username of schema creator. | [optional] [default to null] |
| **updated\_at** | **Long** | Time at which this schema was last modified, in epoch milliseconds. | [optional] [default to null] |
| **updated\_by** | **String** | Username of user who last modified schema. | [optional] [default to null] |
| **schema\_id** | **String** | Unique identifier for the schema. | [optional] [default to null] |
| **storage\_root** | **String** | Storage root URL for managed storage location of schema. This can be set when creating a schema. Example: s3://bucket/ucroot  | [optional] [default to null] |
| **storage\_location** | **String** | Storage Location URL (full path) for managed storage location of schema. This is an automatically generated unique path under storage_root. If it is absent, managed securables under this schema will try to use storage_location of the parent catalog instead. Example: s3://bucket/ucroot/__unitystorage/schemas/{schema_id}  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

