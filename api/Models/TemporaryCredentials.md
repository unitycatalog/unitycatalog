# TemporaryCredentials
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **aws\_temp\_credentials** | [**AwsCredentials**](AwsCredentials.md) |  | [optional] [default to null] |
| **azure\_user\_delegation\_sas** | [**AzureUserDelegationSAS**](AzureUserDelegationSAS.md) |  | [optional] [default to null] |
| **gcp\_oauth\_token** | [**GcpOauthToken**](GcpOauthToken.md) |  | [optional] [default to null] |
| **expiration\_time** | **Long** | Server time when the credential will expire, in epoch milliseconds. The API client is advised to cache the credential given this expiration time.  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

