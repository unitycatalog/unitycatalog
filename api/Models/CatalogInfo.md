# CatalogInfo
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | optional | Name of catalog. | |
| **comment** | **String** | optional | User-provided free-form text description. | |
| **properties** | **Map** | optional | A map of key-value properties attached to the securable. | |
| **owner** | **String** | optional | Username of current owner of catalog. | |
| **created\_at** | **Long** | optional | Time at which this catalog was created, in epoch milliseconds. | |
| **created\_by** | **String** | optional | Username of catalog creator. | |
| **updated\_at** | **Long** | optional | Time at which this catalog was last modified, in epoch milliseconds. | |
| **updated\_by** | **String** | optional | Username of user who last modified catalog. | |
| **id** | **String** | optional | Unique identifier for the catalog. | |
| **storage\_root** | **String** | optional | Storage root URL for managed storage location of catalog. This can be set when creating a catalog. Example: s3://bucket/ucroot  | |
| **storage\_location** | **String** | optional | Storage Location URL (full path) for managed storage location of catalog. This is an automatically generated unique path under storage_root. Example: s3://bucket/ucroot/__unitystorage/catalogs/{catalog_id}  | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

