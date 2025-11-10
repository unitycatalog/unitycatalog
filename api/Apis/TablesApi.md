# TablesApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createStagingTable**](TablesApi.md#createStagingTable) | **POST** /staging-tables | Create a staging table |
| [**createTable**](TablesApi.md#createTable) | **POST** /tables | Create a table. Only external table creation is supported. WARNING: This API is experimental and will change in future versions.  |
| [**deleteTable**](TablesApi.md#deleteTable) | **DELETE** /tables/{full_name} | Delete a table |
| [**getTable**](TablesApi.md#getTable) | **GET** /tables/{full_name} | Get a table |
| [**listTables**](TablesApi.md#listTables) | **GET** /tables | List tables |


<a name="createStagingTable"></a>
# **createStagingTable**
> StagingTableInfo createStagingTable(CreateStagingTable)

Create a staging table

    Creates a new staging table instance. Staging tables are used during managed table creation. Creating a managed table requires performing two actions – initializing the table data in cloud storage and creating the named table entry in the catalog – and these should appear as an atomic operation to other operations on catalog tables. A staging table is used to allocate storage for the managed table, and the catalog_name.schema_name.name parameters provided in this request are used to initialize any required storage properties and determine the storage URL that should be used for the data contained by this table.  Temporary credentials can be obtained as though the staging table were a regular table to get access to the staging table’s storage. After the table’s data is initialized, the staging table is “promoted” to a managed table by creating a managed table with the same location as the staging table. This allows for the atomic creation of a managed table that already has full data written to its storage location. Note: the name provided must match the name used to initialize the staging table originally.  WARNING: This API is experimental and may change in future versions. 

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

<a name="createTable"></a>
# **createTable**
> TableInfo createTable(CreateTable)

Create a table. Only external table creation is supported. WARNING: This API is experimental and will change in future versions. 

    Creates a new external table instance. WARNING: This API is experimental and will change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **CreateTable** | [**CreateTable**](../Models/CreateTable.md)|  | [optional] |

### Return type

[**TableInfo**](../Models/TableInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteTable"></a>
# **deleteTable**
> oas_any_type_not_mapped deleteTable(full\_name)

Delete a table

    Deletes a table from the specified parent catalog and schema. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the table. | [default to null] |

### Return type

[**oas_any_type_not_mapped**](../Models/AnyType.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getTable"></a>
# **getTable**
> TableInfo getTable(full\_name, read\_streaming\_table\_as\_managed, read\_materialized\_view\_as\_managed)

Get a table

    Gets a table for a specific catalog and schema. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the table. | [default to null] |
| **read\_streaming\_table\_as\_managed** | **Boolean**| Whether to read Streaming Tables as Managed tables.  | [optional] [default to true] |
| **read\_materialized\_view\_as\_managed** | **Boolean**| Whether to read Materialized Views as Managed tables.  | [optional] [default to true] |

### Return type

[**TableInfo**](../Models/TableInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listTables"></a>
# **listTables**
> ListTablesResponse listTables(catalog\_name, schema\_name, max\_results, page\_token)

List tables

    Gets the list of all available tables under the parent catalog and schema. There is no guarantee of a specific ordering of the elements in the array. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog\_name** | **String**| Name of parent catalog for tables of interest. | [default to null] |
| **schema\_name** | **String**| Parent schema of tables. | [default to null] |
| **max\_results** | **Integer**| Maximum number of tables to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  | [optional] [default to null] |
| **page\_token** | **String**| Opaque token to send for the next page of results (pagination). | [optional] [default to null] |

### Return type

[**ListTablesResponse**](../Models/ListTablesResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

