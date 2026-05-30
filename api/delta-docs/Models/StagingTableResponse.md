# StagingTableResponse
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **table-id** | **UUID** | required | Table UUID allocated by UC | |
| **table-type** | [**TableType**](TableType.md) | required | Table type (always MANAGED for staging tables) | |
| **location** | **String** | required | UC allocated storage location for this table | |
| **storage-credentials** | [**List**](StorageCredential.md) | required | Temporary credentials for initial commit | |
| **required-protocol** | [**StagingTableResponse_required_protocol**](StagingTableResponse_required_protocol.md) | required |  | |
| **suggested-protocol** | [**StagingTableResponse_suggested_protocol**](StagingTableResponse_suggested_protocol.md) | optional |  | |
| **required-properties** | **Map** | required | Table properties that must be set; null values mean any valid value is allowed | |
| **suggested-properties** | **Map** | optional | Table properties that should be set whenever possible; null values mean client generates the value | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

