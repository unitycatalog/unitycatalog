# DeltaGetCommitsResponse
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **commits** | [**List**](DeltaCommitInfo.md) | The list of unbackfilled Delta table commits. Can be in arbitrary order. | [default to null] |
| **latest\_table\_version** | **Long** | Represents the latest version of the table tracked by UC. If no commits have occurred via UC yet,  then UC cannot determine the latest version and returns -1. Use this field to manage pagination —  if the returned commits don&#39;t cover the range up to latest_table_version or end_version (whichever is smaller),  it indicates that more unbackfilled commits may be available.  | [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

