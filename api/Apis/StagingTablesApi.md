# StagingTablesApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createStagingTable**](StagingTablesApi.md#createStagingTable) | **POST** /staging-tables | Create a staging table |


<a name="createStagingTable"></a>
# **createStagingTable**
> StagingTableInfo createStagingTable(CreateStagingTable)

Create a staging table

    Creates a new staging table instance. Staging tables are used during managed table creation. Creating a managed  table requires performing two actions – initializing the table data in cloud storage and creating the named  table entry in the catalog – and these should appear as an atomic operation to other operations on catalog  tables. A staging table is used to allocate storage for the managed table, and the &#x60;catalog_name.schema_name.name&#x60;  parameters provided in this request are used to initialize any required storage properties and determine the  storage URL that should be used for the data contained by this table.  Temporary credentials can be obtained as though the staging table were a regular table to get access to the  staging table’s storage. After the table’s data is initialized, the staging table is “promoted” to a managed  table by creating a managed table with the same location as the staging table. This allows for the atomic  creation of a managed table that already has full data written to its storage location. Note: the name provided  must match the name used to initialize the staging table originally. 

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

