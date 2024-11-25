# UpdateStorageCredential
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **comment** | **String** | Comment associated with the credential. | [optional] [default to null] |
| **read\_only** | **Boolean** | Whether the storage credential is only usable for read operations. | [optional] [default to null] |
| **owner** | **String** | Username of current owner of credential. | [optional] [default to null] |
| **aws\_iam\_role** | [**AwsIamRoleRequest**](AwsIamRoleRequest.md) |  | [optional] [default to null] |
| **azure\_service\_principal** | [**AzureServicePrincipal**](AzureServicePrincipal.md) |  | [optional] [default to null] |
| **azure\_managed\_identity** | [**AzureManagedIdentityRequest**](AzureManagedIdentityRequest.md) |  | [optional] [default to null] |
| **new\_name** | **String** | New name for the storage credential. | [optional] [default to null] |
| **databricks\_gcp\_service\_account** | [**Object**](.md) |  | [optional] [default to null] |
| **skip\_validation** | **Boolean** | Supplying true to this argument skips validation of the updated credential. | [optional] [default to false] |
| **force** | **Boolean** | Force update even if there are dependent external locations or external tables. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

