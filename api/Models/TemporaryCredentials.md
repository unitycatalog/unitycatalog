# TemporaryCredentials
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **aws\_temp\_credentials** | [**AwsCredentials**](AwsCredentials.md) |  | [optional] [default to null] |
| **azure\_user\_delegation\_sas** | [**AzureUserDelegationSAS**](AzureUserDelegationSAS.md) |  | [optional] [default to null] |
| **gcp\_oauth\_token** | [**GcpOauthToken**](GcpOauthToken.md) |  | [optional] [default to null] |
| **expiration\_time** | **Long** | Server time when the credential will expire, in epoch milliseconds. The API client is advised to cache the credential given this expiration time. EXPERIMENTAL: for S3-compatible storage without &#x60;assume_role&#x60;, it is a refetch time an hour ahead, not when the keys expire: a vended key pair stays valid until the storage rotates or revokes it, and a vend without keys has none to expire. A running client need not fetch again then; the UC Hadoop connector picks up a changed endpoint on its first table load after this time.  | [optional] [default to null] |
| **url** | **String** | The normalized URL of the storage path the temporary credential was generated for. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

