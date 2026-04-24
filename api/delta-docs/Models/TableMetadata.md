# TableMetadata
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **etag** | **String** | Entity tag for optimistic concurrency control | [default to null] |
| **data-source-format** | [**DataSourceFormat**](DataSourceFormat.md) | Data source format (DELTA or ICEBERG) | [default to null] |
| **table-type** | [**TableType**](TableType.md) |  | [default to null] |
| **table-uuid** | **UUID** | Unique identifier for the table | [default to null] |
| **location** | **String** | Storage location of the table | [default to null] |
| **created-time** | **Long** | Creation time in epoch milliseconds | [default to null] |
| **updated-time** | **Long** | Last update time in epoch milliseconds | [default to null] |
| **columns** | [**StructType**](StructType.md) |  | [default to null] |
| **partition-columns** | **List** | Partition column names | [optional] [default to null] |
| **properties** | **Map** | Table properties | [default to null] |
| **last-commit-version** | **Long** | The version of the last commit that changed table metadata (delta.lastUpdateVersion). Data-only commits do not update this value. | [optional] [default to null] |
| **last-commit-timestamp-ms** | **Long** | Timestamp of the last commit that changed table metadata, in epoch milliseconds (delta.lastCommitTimestamp). | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

