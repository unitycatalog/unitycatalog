# LoadTableResponse
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **metadata** | [**TableMetadata**](TableMetadata.md) | Complete table metadata including schema and properties | [default to null] |
| **commits** | [**List**](DeltaCommit.md) | All unbackfilled CCv2 commits | [optional] [default to null] |
| **uniform** | [**UniformMetadata**](UniformMetadata.md) |  | [optional] [default to null] |
| **latest-table-version** | **Long** | The latest ratified table version tracked by the server, including data-only commits. Compare with metadata.last-commit-version which only tracks metadata-changing commits. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

