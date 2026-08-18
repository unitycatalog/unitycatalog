# DeltaLoadTableResponse
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **metadata** | [**DeltaTableMetadata**](DeltaTableMetadata.md) | Complete table metadata including schema and properties | [default to null] |
| **commits** | [**List**](DeltaCommit.md) | All unbackfilled CCv2 commits, in descending version order (newest first). For managed Delta tables the list is complete and contiguous up to latest-table-version and is returned atomically with the metadata; the server bounds its size at write time (ResourceExhaustedException) rather than truncating the response.  | [optional] [default to null] |
| **uniform** | [**DeltaUniformMetadata**](DeltaUniformMetadata.md) |  | [optional] [default to null] |
| **latest-table-version** | **Long** | The latest ratified table version tracked by the server, including data-only commits. Compare with metadata.last-commit-version which only tracks metadata-changing commits. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

