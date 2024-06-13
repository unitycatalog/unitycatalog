# CatalogsApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createCatalog**](CatalogsApi.md#createCatalog) | **POST** /catalogs | Create a catalog |
| [**deleteCatalog**](CatalogsApi.md#deleteCatalog) | **DELETE** /catalogs/{name} | Delete a catalog |
| [**getCatalog**](CatalogsApi.md#getCatalog) | **GET** /catalogs/{name} | Get a catalog |
| [**listCatalogs**](CatalogsApi.md#listCatalogs) | **GET** /catalogs | List catalogs |
| [**updateCatalog**](CatalogsApi.md#updateCatalog) | **PATCH** /catalogs/{name} | Update a catalog |


<a name="createCatalog"></a>
# **createCatalog**
> CatalogInfo createCatalog(CreateCatalog)

Create a catalog

    Creates a new catalog instance. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **CreateCatalog** | [**CreateCatalog**](../Models/CreateCatalog.md)|  | [optional] |

### Return type

[**CatalogInfo**](../Models/CatalogInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteCatalog"></a>
# **deleteCatalog**
> oas_any_type_not_mapped deleteCatalog(name, force)

Delete a catalog

    Deletes the catalog that matches the supplied name. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| The name of the catalog. | [default to null] |
| **force** | **Boolean**| Force deletion even if the catalog is not empty. | [optional] [default to null] |

### Return type

[**oas_any_type_not_mapped**](../Models/AnyType.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getCatalog"></a>
# **getCatalog**
> CatalogInfo getCatalog(name)

Get a catalog

    Gets the specified catalog. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| The name of the catalog. | [default to null] |

### Return type

[**CatalogInfo**](../Models/CatalogInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listCatalogs"></a>
# **listCatalogs**
> ListCatalogsResponse listCatalogs(page\_token, max\_results)

List catalogs

    Lists the available catalogs. There is no guarantee of a specific ordering of the elements in the list. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **page\_token** | **String**| Opaque pagination token to go to next page based on previous query.  | [optional] [default to null] |
| **max\_results** | **Integer**| Maximum number of catalogs to return. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  | [optional] [default to null] |

### Return type

[**ListCatalogsResponse**](../Models/ListCatalogsResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="updateCatalog"></a>
# **updateCatalog**
> CatalogInfo updateCatalog(name, UpdateCatalog)

Update a catalog

    Updates the catalog that matches the supplied name. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| The name of the catalog. | [default to null] |
| **UpdateCatalog** | [**UpdateCatalog**](../Models/UpdateCatalog.md)|  | [optional] |

### Return type

[**CatalogInfo**](../Models/CatalogInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

