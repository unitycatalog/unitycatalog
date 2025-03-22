# AwsIamRoleResponse
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **role\_arn** | **String** | The Amazon Resource Name (ARN) of the AWS IAM role used to vend temporary credentials. | [default to null] |
| **unity\_catalog\_iam\_arn** | **String** | The Amazon Resource Name (ARN) of the AWS IAM used by the Unity Catalog Server. This is the identity that is going to assume the AWS IAM role. | [optional] [default to null] |
| **external\_id** | **String** | The external ID used in role assumption to prevent confused deputy problem. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

