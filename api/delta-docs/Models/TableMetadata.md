# TableMetadata
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **etag** | **String** | required | Entity tag for optimistic concurrency control | |
| **data-source-format** | [**DataSourceFormat**](DataSourceFormat.md) | required | Data source format (DELTA or ICEBERG) | |
| **table-type** | [**TableType**](TableType.md) | required |  | |
| **table-uuid** | **UUID** | required | Unique identifier for the table | |
| **location** | **String** | required | Storage location of the table | |
| **created-time** | **Long** | required | Creation time in epoch milliseconds | |
| **updated-time** | **Long** | required | Last update time in epoch milliseconds | |
| **columns** | [**StructType**](StructType.md) | required |  | |
| **partition-columns** | **List** | optional | Partition column names | |
| **properties** | **Map** | required | Table properties | |
| **last-commit-version** | **Long** | optional | The version of the last commit that changed table metadata (delta.lastUpdateVersion). Data-only commits do not update this value. | |
| **last-commit-timestamp-ms** | **Long** | optional | Timestamp of the last commit that changed table metadata, in epoch milliseconds (delta.lastCommitTimestamp). | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

