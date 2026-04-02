# StagingTableResponse
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **table-id** | **UUID** | Table UUID allocated by UC | [default to null] |
| **table-type** | [**TableType**](TableType.md) | Table type (always MANAGED for staging tables) | [default to null] |
| **location** | **String** | UC allocated storage location for this table | [default to null] |
| **storage-credentials** | [**List**](StorageCredential.md) | Temporary credentials for initial commit | [default to null] |
| **required-protocol** | [**StagingTableResponse_required_protocol**](StagingTableResponse_required_protocol.md) |  | [default to null] |
| **suggested-protocol** | [**StagingTableResponse_suggested_protocol**](StagingTableResponse_suggested_protocol.md) |  | [optional] [default to null] |
| **required-properties** | **Map** | Table properties that must be set; null values mean any valid value is allowed | [default to null] |
| **suggested-properties** | **Map** | Table properties that should be set whenever possible; null values mean client generates the value | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

