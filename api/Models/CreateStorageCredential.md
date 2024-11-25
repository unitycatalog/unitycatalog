# CreateStorageCredential
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | The credential name. The name must be unique within the metastore. | [default to null] |
| **comment** | **String** | Comment associated with the credential. | [optional] [default to null] |
| **read\_only** | **Boolean** | Whether the storage credential is only usable for read operations. | [optional] [default to null] |
| **aws\_iam\_role** | [**AwsIamRoleRequest**](AwsIamRoleRequest.md) |  | [optional] [default to null] |
| **azure\_service\_principal** | [**AzureServicePrincipal**](AzureServicePrincipal.md) |  | [optional] [default to null] |
| **azure\_managed\_identity** | [**AzureManagedIdentityRequest**](AzureManagedIdentityRequest.md) |  | [optional] [default to null] |
| **databricks\_gcp\_service\_account** | [**Object**](.md) |  | [optional] [default to null] |
| **skip\_validation** | **Boolean** | Supplying true to this argument skips validation of the created credential. | [optional] [default to false] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

