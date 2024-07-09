# SchemasApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createSchema**](SchemasApi.md#createSchema) | **POST** /schemas | Create a schema |
| [**deleteSchema**](SchemasApi.md#deleteSchema) | **DELETE** /schemas/{full_name} | Delete a schema |
| [**getSchema**](SchemasApi.md#getSchema) | **GET** /schemas/{full_name} | Get a schema |
| [**listSchemas**](SchemasApi.md#listSchemas) | **GET** /schemas | List schemas |
| [**updateSchema**](SchemasApi.md#updateSchema) | **PATCH** /schemas/{full_name} | Update a schema |


<a name="createSchema"></a>
# **createSchema**
> SchemaInfo createSchema(CreateSchema)

Create a schema

    Creates a new schema in the specified catalog. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **CreateSchema** | [**CreateSchema**](../Models/CreateSchema.md)|  | [optional] |

### Return type

[**SchemaInfo**](../Models/SchemaInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteSchema"></a>
# **deleteSchema**
> oas_any_type_not_mapped deleteSchema(full\_name, force)

Delete a schema

    Deletes the specified schema from the parent catalog. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the schema. | [default to null] |
| **force** | **Boolean**| Force deletion even if the catalog is not empty. | [optional] [default to null] |

### Return type

[**oas_any_type_not_mapped**](../Models/AnyType.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getSchema"></a>
# **getSchema**
> SchemaInfo getSchema(full\_name)

Get a schema

    Gets the specified schema for a catalog. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the schema. | [default to null] |

### Return type

[**SchemaInfo**](../Models/SchemaInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listSchemas"></a>
# **listSchemas**
> ListSchemasResponse listSchemas(catalog\_name, max\_results, page\_token)

List schemas

    Gets an array of schemas for a catalog. There is no guarantee of a specific ordering of the elements in the array. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog\_name** | **String**| Parent catalog for schemas of interest. | [default to null] |
| **max\_results** | **Integer**| Maximum number of schemas to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  | [optional] [default to null] |
| **page\_token** | **String**| Opaque pagination token to go to next page based on previous query.  | [optional] [default to null] |

### Return type

[**ListSchemasResponse**](../Models/ListSchemasResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="updateSchema"></a>
# **updateSchema**
> SchemaInfo updateSchema(full\_name, UpdateSchema)

Update a schema

    Updates the specified schema. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **full\_name** | **String**| Full name of the schema. | [default to null] |
| **UpdateSchema** | [**UpdateSchema**](../Models/UpdateSchema.md)|  | [optional] |

### Return type

[**SchemaInfo**](../Models/SchemaInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

