# S3AssumeRole
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **sts\_endpoint** | **String** | EXPERIMENTAL. STS endpoint of the storage to call AssumeRole on, for example &#x60;https://minio.example.com:9000&#x60;: an http or https URL with a host, and without user info, a query or a fragment; other values fail with &#x60;INVALID_ARGUMENT&#x60;. Absent when not set: AssumeRole then goes to the credential&#39;s &#x60;s3_endpoint&#x60;, so a change of &#x60;s3_endpoint&#x60; moves it too.  | [optional] [default to null] |
| **role\_arn** | **String** | EXPERIMENTAL. The role to assume. | [default to null] |
| **sts\_region** | **String** | EXPERIMENTAL. The region to sign the AssumeRole request with, of letters, digits, &#39;.&#39;, &#39;_&#39; and &#39;-&#39;. If not set, the region of the S3-compatible storage holding the requested path is used.  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

