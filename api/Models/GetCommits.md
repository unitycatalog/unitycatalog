# GetCommits
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **table\_id** | **String** | The table ID provided by UC on table creation that we want to get the commits for. In the context of UC, this is the UUID that uniquely identifies a table. | [default to null] |
| **table\_uri** | **String** | The URI of the storage location of the table. If the table_id exists but the table_uri is  different from the one previously registered (e.g., if the client moved the table), the request will fail.  | [default to null] |
| **start\_version** | **Long** | The start version from which to retrieve commits (inclusive). | [default to null] |
| **end\_version** | **Long** | The end version upto which to retrieve commits (inclusive). If not set, the latest version will be used as the end version. This does not affect the latest_table_version in the response. If num of commits that meet this criteria is &gt; 50, the response will be paginated to the first 50 commits.  | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

