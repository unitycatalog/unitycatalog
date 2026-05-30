# CreateTableRequest
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **name** | **String** | required | The table name | |
| **location** | **String** | required | Storage location | |
| **table-type** | [**TableType**](TableType.md) | required |  | |
| **data-source-format** | [**DataSourceFormat**](DataSourceFormat.md) | required | Data source format (DELTA or ICEBERG) | |
| **comment** | **String** | optional | Table comment | |
| **columns** | [**StructType**](StructType.md) | required |  | |
| **partition-columns** | **List** | optional | Partition column names | |
| **protocol** | [**DeltaProtocol**](DeltaProtocol.md) | required | Delta protocol version and feature requirements | |
| **properties** | **Map** | required | Delta table properties | |
| **domain-metadata** | [**DomainMetadataUpdates**](DomainMetadataUpdates.md) | optional |  | |
| **last-commit-timestamp-ms** | **Long** | required | Timestamp of version 0 (the commit the client wrote  before calling this endpoint), in epoch milliseconds.  | |
| **uniform** | [**UniformMetadata**](UniformMetadata.md) | optional | Optional UniForm conversion metadata. When present, the table is registered as UniForm-enabled, so readers can access it via either the Delta or Iceberg REST Catalog. The engine generates the Iceberg metadata file at the supplied metadata-location before calling createTable.  | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

