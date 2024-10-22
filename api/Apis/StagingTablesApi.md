# StagingTablesApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createStagingTable**](StagingTablesApi.md#createStagingTable) | **POST** /staging-tables | Create a staging table |


<a name="createStagingTable"></a>
# **createStagingTable**
> StagingTableInfo createStagingTable(CreateStagingTable)

Create a staging table

    Creates a new staging table instance. The &#x60;catalog_name.schema_name.name&#x60; parameters provided in this request  are used to initialize any required storage properties and determine the storage URL that should be used for  the data contained by this table. Temporary credentials can be obtained as though the staging table were a regular table.  When a managed table is created with a URL owned by a staging table, that staging table is \&quot;promoted\&quot; into the  newly created table. This allows for the atomic creation of a table that already has full data written to  its storage location. Note: the name provided must match the name used to initialize the staging table originally. 

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

