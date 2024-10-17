# CoordinatedCommitsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**commit**](CoordinatedCommitsApi.md#commit) | **POST** /delta/commits | Commit changes to a specified table |
| [**getCommits**](CoordinatedCommitsApi.md#getCommits) | **GET** /delta/commits | List unbackfilled Delta table commits. |


<a name="commit"></a>
# **commit**
> Object commit(Commit)

Commit changes to a specified table

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
> GetCommitsResponse getCommits(GetCommits)

List unbackfilled Delta table commits.

    List all the unbackfilled delta commits that are currently being tracked by the UC coordinator.  If no commits are being tracked in the specific version range (from start_version to an optional end_version),  it will return an empty list. Please check https://docs.delta.io/latest/delta-coordinated-commits.html for more details 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **GetCommits** | [**GetCommits**](../Models/GetCommits.md)|  | [optional] |

### Return type

[**GetCommitsResponse**](../Models/GetCommitsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

