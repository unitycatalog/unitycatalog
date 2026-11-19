# DeltaStorageCredential_config
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **s3.access-key-id** | **String** | AWS access key ID | [optional] [default to null] |
| **s3.secret-access-key** | **String** | AWS secret access key | [optional] [default to null] |
| **s3.session-token** | **String** | AWS session token for temporary credentials | [optional] [default to null] |
| **s3.endpoint** | **String** | EXPERIMENTAL. S3 API endpoint of the S3-compatible storage holding the prefix, for example &#x60;https://minio.example.com:9000&#x60;: the &#x60;endpoint&#x60; of the external location covering the prefix, addressed as &#x60;s3.path-style-access&#x60; says. Absent for AWS S3. When that credential does not vend keys, this, &#x60;client.region&#x60; and &#x60;s3.path-style-access&#x60; are the only S3 keys set, and the client brings its own. A key pair without &#x60;s3.session-token&#x60;, and the client&#39;s own keys, are not narrowed to the requested operation or prefix; only session credentials are. A client that does not support this key must not use the credential, and must not send its requests to AWS S3.  | [optional] [default to null] |
| **client.region** | **String** | EXPERIMENTAL. Region to sign S3 requests with, set together with &#x60;s3.endpoint&#x60;.  | [optional] [default to null] |
| **s3.path-style-access** | **Boolean** | EXPERIMENTAL. Whether requests to &#x60;s3.endpoint&#x60; name the bucket in the path (&#x60;&lt;endpoint&gt;/&lt;bucket&gt;/&lt;key&gt;&#x60;), rather than in the host name (&#x60;&lt;bucket&gt;.&lt;endpoint host&gt;/&lt;key&gt;&#x60;), as the external location covering the prefix says. Set together with &#x60;s3.endpoint&#x60;.  | [optional] [default to null] |
| **azure.sas-token** | **String** | Azure SAS (Shared Access Signature) token | [optional] [default to null] |
| **gcs.oauth-token** | **String** | GCP OAuth token | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

