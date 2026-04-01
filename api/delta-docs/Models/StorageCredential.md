# StorageCredential
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **prefix** | **String** | Storage path prefix this credential applies to | [default to null] |
| **operation** | [**CredentialOperation**](CredentialOperation.md) |  | [default to null] |
| **config** | [**StorageCredential_config**](StorageCredential_config.md) |  | [default to null] |
| **expiration-time-ms** | **Long** | Credential expiration time in epoch milliseconds. This standardized field avoids the need for provider-specific expiration keys (e.g., s3.session-token-expires-at-ms, adls.sas-token-expires-at-ms, etc.)  | [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

