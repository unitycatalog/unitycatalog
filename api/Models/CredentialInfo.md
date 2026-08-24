# CredentialInfo
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | The credential name. The name must be unique within the metastore. | [optional] [default to null] |
| **aws\_iam\_role** | [**AwsIamRoleResponse**](AwsIamRoleResponse.md) |  | [optional] [default to null] |
| **comment** | **String** | Comment associated with the credential. | [optional] [default to null] |
| **owner** | **String** | Username of current owner of credential. | [optional] [default to null] |
| **full\_name** | **String** | The full name of the credential. | [optional] [default to null] |
| **id** | **String** | The unique identifier of the credential. | [optional] [default to null] |
| **created\_at** | **Long** | Time at which this Credential was created, in epoch milliseconds. | [optional] [default to null] |
| **created\_by** | **String** | Username of credential creator. | [optional] [default to null] |
| **updated\_at** | **Long** | Time at which this credential was last modified, in epoch milliseconds. | [optional] [default to null] |
| **updated\_by** | **String** | Username of user who last modified the credential. | [optional] [default to null] |
| **purpose** | [**CredentialPurpose**](CredentialPurpose.md) |  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

