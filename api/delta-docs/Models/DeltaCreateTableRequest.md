# DeltaCreateTableRequest
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **name** | **String** | The table name | [default to null] |
| **location** | **String** | Storage location. A cloud location is URI-encoded: the folder &#x60;team reports&#x60; is &#x60;team%20reports&#x60; and a folder named &#x60;team%20reports&#x60; is &#x60;team%2520reports&#x60;. A location containing &#x60;?&#x60; or &#x60;#&#x60; is rejected. | [default to null] |
| **table-type** | [**DeltaTableType**](DeltaTableType.md) |  | [default to null] |
| **comment** | **String** | Table comment | [optional] [default to null] |
| **columns** | [**DeltaStructType**](DeltaStructType.md) |  | [default to null] |
| **partition-columns** | **List** | Partition column names | [optional] [default to null] |
| **protocol** | [**DeltaProtocol**](DeltaProtocol.md) | Delta protocol version and feature requirements | [default to null] |
| **properties** | **Map** | Delta table properties | [default to null] |
| **domain-metadata** | [**DeltaDomainMetadataUpdates**](DeltaDomainMetadataUpdates.md) |  | [optional] [default to null] |
| **last-commit-timestamp-ms** | **Long** | Timestamp of version 0 (the commit the client wrote  before calling this endpoint), in epoch milliseconds.  | [default to null] |
| **uniform** | [**DeltaUniformMetadata**](DeltaUniformMetadata.md) | Optional UniForm conversion metadata. When present, the table is registered as UniForm-enabled, so readers can access it via either the Delta or Iceberg REST Catalog. The engine generates the Iceberg metadata file at the supplied metadata-location before calling createTable.  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

