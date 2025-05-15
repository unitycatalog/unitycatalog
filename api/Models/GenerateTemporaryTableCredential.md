# GenerateTemporaryTableCredential
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **table\_id** | **String** | Table id for which temporary credentials are generated.  Can be obtained from tables/{full_name} (get table info) API.  | [default to null] |
| **operation** | [**TableOperation**](TableOperation.md) |  | [default to null] |
| **read\_st\_as\_managed** | [**oas_any_type_not_mapped**](.md) | Whether to read Streaming Tables as Managed tables. type: boolean  | [optional] [default to null] |
| **read\_mv\_as\_managed** | **Boolean** | Whether to read Materialized Views as Managed tables.  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

