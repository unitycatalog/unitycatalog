# StorageCredential
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **prefix** | **String** | required | Storage path prefix this credential applies to | |
| **operation** | [**CredentialOperation**](CredentialOperation.md) | required |  | |
| **config** | [**StorageCredential_config**](StorageCredential_config.md) | required |  | |
| **expiration-time-ms** | **Long** | required | Credential expiration time in epoch milliseconds. This standardized field avoids the need for provider-specific expiration keys (e.g., s3.session-token-expires-at-ms, adls.sas-token-expires-at-ms, etc.)  | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

