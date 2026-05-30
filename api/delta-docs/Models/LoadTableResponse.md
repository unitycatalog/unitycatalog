# LoadTableResponse
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **metadata** | [**TableMetadata**](TableMetadata.md) | required | Complete table metadata including schema and properties | |
| **commits** | [**List**](DeltaCommit.md) | optional | All unbackfilled CCv2 commits | |
| **uniform** | [**UniformMetadata**](UniformMetadata.md) | optional |  | |
| **latest-table-version** | **Long** | optional | The latest ratified table version tracked by the server, including data-only commits. Compare with metadata.last-commit-version which only tracks metadata-changing commits. | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

