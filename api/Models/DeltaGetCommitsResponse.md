# DeltaGetCommitsResponse
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **commits** | [**List**](DeltaCommitInfo.md) | required | The list of unbackfilled Delta table commits. Can be in arbitrary order. | |
| **latest\_table\_version** | **Long** | required | Represents the latest version of the table tracked by UC. For a newly created managed table with no commits, this returns 0. Use this field to manage pagination — if the returned commits don&#39;t cover the range up to latest_table_version or end_version (whichever is smaller), it indicates that more unbackfilled commits may be available.  | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

