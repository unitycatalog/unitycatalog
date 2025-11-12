# DeltaCommitsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**commit**](DeltaCommitsApi.md#commit) | **POST** /delta/preview/commits | Commit changes to a specified Delta table. The server has a limit defined in config on how many unbackfilled commits it can hold. Clients are expected to do active backfill of the commit after committing to UC. So in most cases the number of unbackfilled commits should be close to zero or one. But if clients misbehave and unbackfilled commits accumulate beyond the limit, server will reject further commits until more backfill is done. WARNING: This API is experimental and may change in future versions.  |
| [**getCommits**](DeltaCommitsApi.md#getCommits) | **GET** /delta/preview/commits | List unbackfilled Delta table commits. WARNING: This API is experimental and may change in future versions.  |


<a name="commit"></a>
# **commit**
> Object commit(DeltaCommit)

Commit changes to a specified Delta table. The server has a limit defined in config on how many unbackfilled commits it can hold. Clients are expected to do active backfill of the commit after committing to UC. So in most cases the number of unbackfilled commits should be close to zero or one. But if clients misbehave and unbackfilled commits accumulate beyond the limit, server will reject further commits until more backfill is done. WARNING: This API is experimental and may change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **DeltaCommit** | [**DeltaCommit**](../Models/DeltaCommit.md)|  | |

### Return type

**Object**

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="getCommits"></a>
# **getCommits**
> DeltaGetCommitsResponse getCommits(DeltaGetCommits)

List unbackfilled Delta table commits. WARNING: This API is experimental and may change in future versions. 

    List all the unbackfilled Delta commits that are currently being tracked by the UC coordinator. If no commits are being tracked in the specific version range (from start_version to an optional end_version), it will return an empty list. WARNING: This API is experimental and may change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **DeltaGetCommits** | [**DeltaGetCommits**](../Models/DeltaGetCommits.md)|  | [optional] |

### Return type

[**DeltaGetCommitsResponse**](../Models/DeltaGetCommitsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

