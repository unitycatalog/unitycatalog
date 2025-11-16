# DeltaCommit
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **table\_id** | **String** | The ID of the table to commit to. This ID uniquely identifies a table. | [default to null] |
| **table\_uri** | **String** | The URI of the storage location of the table. If the table_id exists but the table_uri is  different from the one previously registered (e.g., if the client moved the table), the request will fail. Example: s3://bucket-name/tables/some-table-id  | [default to null] |
| **commit\_info** | [**DeltaCommitInfo**](DeltaCommitInfo.md) |  | [optional] [default to null] |
| **latest\_backfilled\_version** | **Long** | The highest version of the commits that have been backfilled for this table; meaning UC no longer  needs to keep track of commits of versions &lt;&#x3D; this version.  | [optional] [default to null] |
| **metadata** | [**DeltaMetadata**](DeltaMetadata.md) |  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

