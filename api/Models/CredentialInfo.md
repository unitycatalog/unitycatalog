# CredentialInfo
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | optional | The credential name. The name must be unique within the metastore. | |
| **aws\_iam\_role** | [**AwsIamRoleResponse**](AwsIamRoleResponse.md) | optional |  | |
| **comment** | **String** | optional | Comment associated with the credential. | |
| **owner** | **String** | optional | Username of current owner of credential. | |
| **full\_name** | **String** | optional | The full name of the credential. | |
| **id** | **String** | optional | The unique identifier of the credential. | |
| **created\_at** | **Long** | optional | Time at which this Credential was created, in epoch milliseconds. | |
| **created\_by** | **String** | optional | Username of credential creator. | |
| **updated\_at** | **Long** | optional | Time at which this credential was last modified, in epoch milliseconds. | |
| **updated\_by** | **String** | optional | Username of user who last modified the credential. | |
| **purpose** | [**CredentialPurpose**](CredentialPurpose.md) | optional |  | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

