# CreateExternalLocation
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | Name of the external location. | [default to null] |
| **url** | **String** | Path URL of the external location. | [default to null] |
| **credential\_name** | **String** | Name of the storage credential used with this location. | [default to null] |
| **read\_only** | **Boolean** | Indicates whether the external location is read-only. | [optional] [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |
| **access\_point** | **String** | The AWS access point to use when accessing s3 for this external location. | [optional] [default to null] |
| **skip\_validation** | **Boolean** | Skips validation of the storage credential associated with the external location. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

