# DeltaUniformMetadata_iceberg
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **metadata-location** | **String** | Iceberg metadata file location. A cloud location is URI-encoded: the folder &#x60;team reports&#x60; is &#x60;team%20reports&#x60; and a folder named &#x60;team%20reports&#x60; is &#x60;team%2520reports&#x60;. A location containing &#x60;?&#x60; or &#x60;#&#x60; is rejected. | [optional] [default to null] |
| **converted-delta-version** | **Long** | Delta version that was converted to Iceberg | [optional] [default to null] |
| **converted-delta-timestamp** | **Long** | Timestamp of the conversion, in epoch milliseconds | [optional] [default to null] |
| **base-converted-delta-version** | **Long** | Delta version used as the base for incremental conversion | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

