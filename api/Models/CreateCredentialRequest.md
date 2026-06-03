# CreateCredentialRequest
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | required | The credential name. The name must be unique within the metastore. | |
| **comment** | **String** | optional | Comment associated with the credential. | |
| **aws\_iam\_role** | [**AwsIamRoleRequest**](AwsIamRoleRequest.md) | optional |  | |
| **purpose** | [**CredentialPurpose**](CredentialPurpose.md) | optional |  | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

