# S3CompatibleRequest
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **s3\_endpoint** | **String** | EXPERIMENTAL. S3 API endpoint of the S3-compatible storage, for example &#x60;https://minio.example.com:9000&#x60;: an http or https URL with a host, and without user info, a query or a fragment; other values fail with &#x60;INVALID_ARGUMENT&#x60;. Credential vending returns it with the region engines need to reach the storage; requests to it use path-style addressing.  | [default to null] |
| **access\_key\_id** | **String** | EXPERIMENTAL. The access key ID. | [default to null] |
| **secret\_access\_key** | **String** | EXPERIMENTAL. The secret access key. Write-only; never returned. | [default to null] |
| **assume\_role** | [**S3AssumeRole**](S3AssumeRole.md) |  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

