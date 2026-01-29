# DeltaUniformIceberg
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **metadata\_location** | **URI** | The latest Iceberg metadata location. Example: s3://abc/def/metadata/v1.json  | [default to null] |
| **converted\_delta\_version** | **Long** | The converted Delta version corresponding to the metadata location. It should match the Delta version in the commit info. Example: 1044  | [default to null] |
| **converted\_delta\_timestamp** | **String** | The timestamp when the corresponding conversion happened. The string must represent a valid instant in UTC with ISO 8601 format Example: 2025-01-04T03:13:11.423Z  | [default to null] |
| **base\_converted\_delta\_version** | **Long** | Optional version indicating the converted Delta version for the last conversion before the conversion corresponding to the metadata location. Example: 1042  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

