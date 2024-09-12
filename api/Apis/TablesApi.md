# TablesApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createTable**](TablesApi.md#createTable) | **POST** /tables | Create a table. Only external table creation is supported. WARNING: This API is experimental and will change in future versions.  |
| [**deleteTable**](TablesApi.md#deleteTable) | **DELETE** /tables/{full_name} | Delete a table |
| [**getTable**](TablesApi.md#getTable) | **GET** /tables/{full_name} | Get a table |
| [**listTables**](TablesApi.md#listTables) | **GET** /tables | List tables |


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
> TableInfo getTable(full\_name)

Get a table

    Gets a table for a specific catalog and schema. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the table. | [default to null] |

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

