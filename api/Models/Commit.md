# Commit
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **table\_id** | **String** | The ID of the table to commit to. In the context of UC, this is the UUID that uniquely identifies a table. | [default to null] |
| **table\_uri** | **String** | The URI of the storage location of the table. If the table_id exists but the table_uri is  different from the one previously registered (e.g., if the client moved the table), the request will fail.  | [default to null] |
| **commit\_info** | [**CommitInfo**](CommitInfo.md) | The CommitInfo for this request. At least one of commit_info and latest_backfilled_version must be present. | [optional] [default to null] |
| **latest\_backfilled\_version** | **Long** | The highest version of the commits that have been backfilled for this table; meaning UC no longer  needs to keep track of commits of versions &lt;&#x3D; this version. At least one of commit_info and  latest_backfilled_version must be present.  | [optional] [default to null] |
| **metadata** | [**Metadata**](Metadata.md) | Contains metadata action of the commit. It is required in case the metadata changed.  This includes schema changes and other table metadata updates. These changes are applied atomically  with the commit. Metadata updates can be blocked if deemed inappropriate during the commit process.  | [optional] [default to null] |
| **protocol** | [**Protocol**](Protocol.md) | Contains protocol changes for the commit. It is required in case the protocol changed. These changes are  applied atomically with the commit. Protocol updates can be blocked if deemed inappropriate during the  commit process.  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

