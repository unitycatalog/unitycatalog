# StorageCredentialInfo
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | The credential name. The name must be unique within the metastore. | [optional] [default to null] |
| **aws\_iam\_role** | [**AwsIamRoleResponse**](AwsIamRoleResponse.md) |  | [optional] [default to null] |
| **azure\_service\_principal** | [**AzureServicePrincipal**](AzureServicePrincipal.md) |  | [optional] [default to null] |
| **azure\_managed\_identity** | [**AzureManagedIdentityResponse**](AzureManagedIdentityResponse.md) |  | [optional] [default to null] |
| **databricks\_gcp\_service\_account** | [**DatabricksGcpServiceAccountResponse**](DatabricksGcpServiceAccountResponse.md) |  | [optional] [default to null] |
| **comment** | **String** | Comment associated with the credential. | [optional] [default to null] |
| **read\_only** | **Boolean** | Whether the storage credential is only usable for read operations. | [optional] [default to null] |
| **owner** | **String** | Username of current owner of credential. | [optional] [default to null] |
| **full\_name** | **String** | The full name of the credential. | [optional] [default to null] |
| **id** | **String** | The unique identifier of the credential. | [optional] [default to null] |
| **created\_at** | **Long** | Time at which this Credential was created, in epoch milliseconds. | [optional] [default to null] |
| **created\_by** | **String** | Username of credential creator. | [optional] [default to null] |
| **updated\_at** | **Long** | Time at which this credential was last modified, in epoch milliseconds. | [optional] [default to null] |
| **updated\_by** | **String** | Username of user who last modified the credential. | [optional] [default to null] |
| **used\_for\_managed\_storage** | **Boolean** | Whether this credential is the current metastore&#39;s root storage credential. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

