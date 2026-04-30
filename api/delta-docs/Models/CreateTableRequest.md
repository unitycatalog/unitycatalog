# CreateTableRequest
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | The table name | [default to null] |
| **location** | **String** | Storage location | [default to null] |
| **table-type** | [**TableType**](TableType.md) |  | [default to null] |
| **data-source-format** | [**DataSourceFormat**](DataSourceFormat.md) | Data source format (DELTA or ICEBERG) | [default to null] |
| **comment** | **String** | Table comment | [optional] [default to null] |
| **columns** | [**StructType**](StructType.md) |  | [default to null] |
| **partition-columns** | **List** | Partition column names | [optional] [default to null] |
| **protocol** | [**DeltaProtocol**](DeltaProtocol.md) | Delta protocol version and feature requirements | [default to null] |
| **properties** | **Map** | Delta table properties | [default to null] |
| **domain-metadata** | [**DomainMetadataUpdates**](DomainMetadataUpdates.md) |  | [optional] [default to null] |
| **uniform** | [**UniformMetadata**](UniformMetadata.md) | Optional UniForm conversion metadata. When present, the table is registered as UniForm-enabled, so readers can access it via either the Delta or Iceberg REST Catalog. The engine generates the Iceberg metadata file at the supplied metadata-location before calling createTable.  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

