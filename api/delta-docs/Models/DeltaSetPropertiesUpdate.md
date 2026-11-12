# DeltaSetPropertiesUpdate
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **action** | **String** |  | [optional] [default to null] |
| **updates** | **Map** | Properties to set. On a MANAGED table, a server-derived key (delta.minReaderVersion, delta.minWriterVersion, delta.feature.*, clusteringColumns, delta.rowTracking.rowIdHighWaterMark, delta.lastUpdateVersion, delta.lastCommitTimestamp) is rejected with a 400 InvalidParameterValueException, and a change to a required catalog-managed property or io.unitycatalog.tableId that breaks the contract is rejected with the same error. Resending a required property with its stored value is accepted.  | [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

