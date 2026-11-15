# DeltaStorageCredential
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **prefix** | **String** | Storage path prefix this credential applies to | [default to null] |
| **operation** | [**DeltaCredentialOperation**](DeltaCredentialOperation.md) |  | [default to null] |
| **config** | [**DeltaStorageCredential_config**](DeltaStorageCredential_config.md) |  | [default to null] |
| **expiration-time-ms** | **Long** | Credential expiration time in epoch milliseconds. This standardized field avoids the need for provider-specific expiration keys (e.g., s3.session-token-expires-at-ms, adls.sas-token-expires-at-ms, etc.) EXPERIMENTAL: when the config has &#x60;s3.endpoint&#x60; but no &#x60;s3.session-token&#x60;, it is a refetch time an hour ahead, not when the keys expire: a key pair without a session token stays valid until the storage rotates or revokes it, and a config without keys has none to expire. A running client need not fetch again then; the UC Hadoop connector picks up a changed endpoint on its first table load after this time.  | [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

