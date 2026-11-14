# AwsCredentials
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **access\_key\_id** | **String** | The access key ID that identifies the temporary credentials. | [optional] [default to null] |
| **secret\_access\_key** | **String** | The secret access key that can be used to sign AWS API requests. | [optional] [default to null] |
| **session\_token** | **String** | The token that users must pass to AWS API to use the temporary credentials. | [optional] [default to null] |
| **s3\_endpoint** | **String** | EXPERIMENTAL. The S3 API endpoint to send requests for the path to, set when the path is on S3-compatible storage. It is the &#x60;s3_endpoint&#x60; of the storage credential of the external location covering the path. Requests to it always use path-style addressing. Absent for AWS S3. When that credential does not vend keys, this and &#x60;s3_region&#x60; are the only fields set. A client that does not support this field must not use the credentials, and must not send their requests to AWS S3. | [optional] [default to null] |
| **s3\_region** | **String** | EXPERIMENTAL. The region to sign S3 requests with, set together with &#x60;s3_endpoint&#x60;. Unity Catalog asks the storage for the bucket&#39;s region, and uses &#x60;us-east-1&#x60; if it does not say. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

