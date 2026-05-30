# DeltaUniformIceberg
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **metadata\_location** | **URI** | required | The latest Iceberg metadata location. Example: s3://abc/def/metadata/v1.json  | |
| **converted\_delta\_version** | **Long** | required | The Delta version that was converted to Iceberg to produce the Iceberg metadata location. It should match the Delta version in the commit info. Example: 1044  | |
| **converted\_delta\_timestamp** | **String** | required | The timestamp that Delta finished conversion to produce the Iceberg metadata location. The string must represent a valid instant in UTC with ISO 8601 format. Example: 2025-01-04T03:13:11.423Z  | |
| **base\_converted\_delta\_version** | **Long** | optional | Optional Delta version used to incrementally convert Delta changes to Iceberg changes to produce the latest Iceberg metadata at the metadata_location. Example: 1042  | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

