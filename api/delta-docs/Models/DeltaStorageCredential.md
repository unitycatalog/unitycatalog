# DeltaStorageCredential
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **prefix** | **String** | Storage path prefix this credential applies to | [default to null] |
| **operation** | [**DeltaCredentialOperation**](DeltaCredentialOperation.md) |  | [default to null] |
| **config** | [**DeltaStorageCredential_config**](DeltaStorageCredential_config.md) |  | [default to null] |
| **expiration-time-ms** | **Long** | Credential expiration time in epoch milliseconds. This standardized field avoids the need for provider-specific expiration keys (e.g., s3.session-token-expires-at-ms, adls.sas-token-expires-at-ms, etc.) EXPERIMENTAL: for S3-compatible storage without &#x60;assume_role&#x60;, it is when to fetch credentials again, an hour ahead, in both of Unity Catalog&#39;s modes for it (&#x60;server.s3-compatible-storage.static-key-vending&#x60;): a &#x60;static-key&#x60; key pair itself stays valid until the storage rotates or revokes it, and an &#x60;endpoint-only&#x60; vend carries no keys. An engine that does not fetch a vend without keys again, such as the UC Hadoop connector, picks up a changed endpoint on its first table load after this time.  | [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

