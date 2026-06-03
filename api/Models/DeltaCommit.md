# DeltaCommit
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **table\_id** | **String** | required | The ID of the table to commit to. This ID uniquely identifies a table. | |
| **table\_uri** | **String** | required | The URI of the storage location of the table. If the table_id exists but the table_uri is  different from the one previously registered (e.g., if the client moved the table), the request will fail. Example: s3://bucket-name/tables/some-table-id  | |
| **commit\_info** | [**DeltaCommitInfo**](DeltaCommitInfo.md) | optional |  | |
| **latest\_backfilled\_version** | **Long** | optional | The highest version of the commits that have been backfilled for this table; meaning UC no longer  needs to keep track of commits of versions &lt;&#x3D; this version.  | |
| **metadata** | [**DeltaMetadata**](DeltaMetadata.md) | optional |  | |
| **uniform** | [**DeltaUniform**](DeltaUniform.md) | optional |  | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

