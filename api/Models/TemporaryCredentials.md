# TemporaryCredentials
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **aws\_temp\_credentials** | [**AwsCredentials**](AwsCredentials.md) | optional |  | |
| **azure\_user\_delegation\_sas** | [**AzureUserDelegationSAS**](AzureUserDelegationSAS.md) | optional |  | |
| **gcp\_oauth\_token** | [**GcpOauthToken**](GcpOauthToken.md) | optional |  | |
| **expiration\_time** | **Long** | optional | Server time when the credential will expire, in epoch milliseconds. The API client is advised to cache the credential given this expiration time.  | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

