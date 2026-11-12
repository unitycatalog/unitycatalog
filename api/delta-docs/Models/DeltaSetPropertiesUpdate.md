# DeltaSetPropertiesUpdate
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **action** | **String** |  | [optional] [default to null] |
| **updates** | **Map** | Properties to set. On a MANAGED table, a server-derived key (delta.minReaderVersion, delta.minWriterVersion, delta.feature.*, clusteringColumns, delta.rowTracking.rowIdHighWaterMark, delta.lastUpdateVersion, delta.lastCommitTimestamp) is rejected with a 400 InvalidParameterValueException. Changing the stored value of a required catalog-managed property, io.unitycatalog.tableId or delta.enableIcebergCompatV2 is rejected with the same error when the result breaks the contract. Resending the stored value of a contract property is a no-op.  | [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

