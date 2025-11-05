# CoordinatedCommitsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**commit**](CoordinatedCommitsApi.md#commit) | **POST** /delta/preview/commits | Commit changes to a specified table. The server has a limit defined in config on how many unbackfilled commits it can hold. Clients are expected to do active backfill of the commit after committing to UC. So in most cases the number of unbackfilled commits should be close to zero or one. But if clients misbehave and unbackfilled commits accumulate beyond the limit, server will reject further commits until more backfill is done. WARNING: This API is experimental and may change in future versions.  |
| [**getCommits**](CoordinatedCommitsApi.md#getCommits) | **GET** /delta/preview/commits | List unbackfilled Delta table commits. WARNING: This API is experimental and may change in future versions.  |


<a name="commit"></a>
# **commit**
> Object commit(Commit)

Commit changes to a specified table. The server has a limit defined in config on how many unbackfilled commits it can hold. Clients are expected to do active backfill of the commit after committing to UC. So in most cases the number of unbackfilled commits should be close to zero or one. But if clients misbehave and unbackfilled commits accumulate beyond the limit, server will reject further commits until more backfill is done. WARNING: This API is experimental and may change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **Commit** | [**Commit**](../Models/Commit.md)|  | |

### Return type

**Object**

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="getCommits"></a>
# **getCommits**
> GetCommitsResponse getCommits(table\_id, table\_uri, start\_version, end\_version)

List unbackfilled Delta table commits. WARNING: This API is experimental and may change in future versions. 

    List all the unbackfilled delta commits that are currently being tracked by the UC coordinator.  If no commits are being tracked in the specific version range (from start_version to an optional end_version),  it will return an empty list. WARNING: This API is experimental and may change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **table\_id** | **String**| The ID of the table to get the commits for. This ID uniquely identifies a table. | [default to null] |
| **table\_uri** | **String**| The URI of the storage location of the table. If the table_id exists but the table_uri is  different from the one previously registered (e.g., if the client moved the table), the request will fail. Example: s3://bucket-name/tables/some-table-id  | [default to null] |
| **start\_version** | **Long**| The start version from which to retrieve commits (inclusive). This along with the optional end_version specifies the range of commit versions that this request wants.  | [default to null] |
| **end\_version** | **Long**| The end version upto which to retrieve commits (inclusive). If not set, the latest version will be used as the end version. This does not affect the latest_table_version in the response. If num of commits that meet this criteria is larger than a limit set by server config, the response will be limited to the first X commits. Call can send request again with a larger start_version according to the response to get the remaining commits.  | [optional] [default to null] |

### Return type

[**GetCommitsResponse**](../Models/GetCommitsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

