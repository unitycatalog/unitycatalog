# StagingTablesApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createStagingTable**](StagingTablesApi.md#createStagingTable) | **POST** /staging-tables | Create a staging table |


<a name="createStagingTable"></a>
# **createStagingTable**
> StagingTableInfo createStagingTable(CreateStagingTable)

Create a staging table

    Creates a new staging table instance. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **CreateStagingTable** | [**CreateStagingTable**](../Models/CreateStagingTable.md)|  | [optional] |

### Return type

[**StagingTableInfo**](../Models/StagingTableInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

