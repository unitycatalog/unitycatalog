# DeltaRemovePropertiesUpdate
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **action** | **String** |  | [default to null] |
| **removals** | **List** | Property keys to remove. On a MANAGED table, removing a server-derived key (see set-properties) is rejected with a 400 InvalidParameterValueException. Removing a stored required catalog-managed property, io.unitycatalog.tableId or delta.enableIcebergCompatV2 is rejected with the same error when the result breaks the contract. Removing a contract key that is not stored is a no-op.  | [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

