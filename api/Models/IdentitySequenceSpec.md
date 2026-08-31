# IdentitySequenceSpec
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **sequence\_id** | **String** | The client-minted unique id (a UUID) that names the sequence. It is written into the Delta column metadata as the counter pointer. Must be non-empty and at most 64 characters. | [default to null] |
| **start** | **Long** | The first value the sequence issues. | [default to null] |
| **step** | **Long** | The increment between successive values. Must be non-zero. A negative step produces a descending sequence. | [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

