# IdentityReservation
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **sequence\_id** | **String** | The id of the sequence to reserve from. | [default to null] |
| **count** | **Long** | The number of values to reserve. Must be positive. | [default to null] |
| **step** | **Long** | Optional advisory step. If set, it must match the sequence&#39;s stored step. Else a mismatch is rejected so a stale caller cannot silently reserve values under the wrong stride.  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

