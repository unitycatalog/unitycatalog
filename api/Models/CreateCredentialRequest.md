# CreateCredentialRequest
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | The credential name. The name must be unique within the metastore. | [default to null] |
| **comment** | **String** | Comment associated with the credential. | [optional] [default to null] |
| **aws\_iam\_role** | [**AwsIamRoleRequest**](AwsIamRoleRequest.md) |  | [optional] [default to null] |
| **s3\_compatible** | [**S3CompatibleRequest**](S3CompatibleRequest.md) |  | [optional] [default to null] |
| **gcp\_service\_account\_key** | [**GcpServiceAccountKeyRequest**](GcpServiceAccountKeyRequest.md) |  | [optional] [default to null] |
| **gcp\_server\_identity** | [**Object**](.md) | Configures a storage credential that uses the Unity Catalog server&#39;s own Google Cloud Application Default Credentials (for example, a GKE Workload Identity or Compute Engine service account). Pass an empty JSON object (&#x60;{}&#x60;). No key is sent or stored, and &#x60;server.credential-encryption-key&#x60; is not required. Its external locations must be &#x60;gs://&#x60; paths. Creating one checks that Application Default Credentials are available on the server and fails with &#x60;FAILED_PRECONDITION&#x60; if not.  | [optional] [default to null] |
| **purpose** | [**CredentialPurpose**](CredentialPurpose.md) |  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

