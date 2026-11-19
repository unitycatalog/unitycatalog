# CreateExternalLocation
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | Name of the external location. | [default to null] |
| **url** | **String** | Path URL of the external location. | [default to null] |
| **credential\_name** | **String** | Name of the storage credential used with this location. | [default to null] |
| **endpoint** | **String** | EXPERIMENTAL. S3 API endpoint of the S3-compatible storage the external location is on, for example &#x60;https://minio.example.com:9000&#x60;: an http or https URL with a host, and without user info, a query or a fragment; other values fail with &#x60;INVALID_ARGUMENT&#x60;. Set exactly when the location&#39;s credential is &#x60;s3_compatible_access_key&#x60;. Credential vending returns it with the region and the addressing style engines need to reach the storage.  | [optional] [default to null] |
| **addressing\_style** | [**S3AddressingStyle**](S3AddressingStyle.md) |  | [optional] [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

