# DeltaStorageCredential_config
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **s3.access-key-id** | **String** | AWS access key ID | [optional] [default to null] |
| **s3.secret-access-key** | **String** | AWS secret access key | [optional] [default to null] |
| **s3.session-token** | **String** | AWS session token for temporary credentials | [optional] [default to null] |
| **s3.endpoint** | **String** | EXPERIMENTAL. S3 API endpoint of the S3-compatible storage holding the prefix, for example &#x60;https://minio.example.com:9000&#x60;: the &#x60;s3_endpoint&#x60; of the storage credential of the external location covering the prefix. Requests to it always use path-style addressing. Absent for AWS S3. When that credential does not vend keys, this and &#x60;client.region&#x60; are the only S3 keys set. A client that does not support this key must not use the credential, and must not send its requests to AWS S3.  | [optional] [default to null] |
| **client.region** | **String** | EXPERIMENTAL. Region to sign S3 requests with, set together with &#x60;s3.endpoint&#x60;.  | [optional] [default to null] |
| **azure.sas-token** | **String** | Azure SAS (Shared Access Signature) token | [optional] [default to null] |
| **gcs.oauth-token** | **String** | GCP OAuth token | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

