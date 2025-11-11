# GetCommits
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **table\_id** | **String** | The ID of the table to get the commits for. This ID uniquely identifies a table. | [default to null] |
| **table\_uri** | **String** | The URI of the storage location of the table. If the table_id exists but the table_uri is  different from the one previously registered (e.g., if the client moved the table), the request will fail. Example: s3://bucket-name/tables/some-table-id  | [default to null] |
| **start\_version** | **Long** | The start version from which to retrieve commits (inclusive). This along with the optional end_version specifies the range of commit versions that this request wants.  | [default to null] |
| **end\_version** | **Long** | The end version upto which to retrieve commits (inclusive). If not set, the latest version will be used as the end version. This does not affect the latest_table_version in the response. If num of commits that meet this criteria is larger than a limit set by server config, the response will be limited to the first X commits. Call can send request again with a larger start_version according to the response to get the remaining commits.  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

