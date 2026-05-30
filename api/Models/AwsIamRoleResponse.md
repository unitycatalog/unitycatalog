# AwsIamRoleResponse
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **role\_arn** | **String** | required | The Amazon Resource Name (ARN) of the AWS IAM role used to vend temporary credentials. | |
| **unity\_catalog\_iam\_arn** | **String** | optional | The Amazon Resource Name (ARN) of the AWS IAM used by the Unity Catalog Server. This is the identity that is going to assume the AWS IAM role. | |
| **external\_id** | **String** | optional | The external ID used in role assumption to prevent confused deputy problem. | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

