# VolumesApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createVolume**](VolumesApi.md#createVolume) | **POST** /volumes | Create a Volume |
| [**deleteVolume**](VolumesApi.md#deleteVolume) | **DELETE** /volumes/{name} | Delete a Volume |
| [**getVolume**](VolumesApi.md#getVolume) | **GET** /volumes/{name} | Get a Volume |
| [**listVolumes**](VolumesApi.md#listVolumes) | **GET** /volumes | List Volumes |
| [**updateVolume**](VolumesApi.md#updateVolume) | **PATCH** /volumes/{name} | Update a Volume |


<a name="createVolume"></a>
# **createVolume**
> VolumeInfo createVolume(CreateVolumeRequestContent)

Create a Volume

    Creates a new volume. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **CreateVolumeRequestContent** | [**CreateVolumeRequestContent**](../Models/CreateVolumeRequestContent.md)|  | |

### Return type

[**VolumeInfo**](../Models/VolumeInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="deleteVolume"></a>
# **deleteVolume**
> oas_any_type_not_mapped deleteVolume(name)

Delete a Volume

    Deletes a volume from the specified parent catalog and schema. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| The three-level (fully qualified) name of the volume | [default to null] |

### Return type

[**oas_any_type_not_mapped**](../Models/AnyType.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="getVolume"></a>
# **getVolume**
> VolumeInfo getVolume(name)

Get a Volume

    Gets a volume for a specific catalog and schema. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| The three-level (fully qualified) name of the volume | [default to null] |

### Return type

[**VolumeInfo**](../Models/VolumeInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="listVolumes"></a>
# **listVolumes**
> ListVolumesResponseContent listVolumes(catalog\_name, schema\_name, max\_results, page\_token)

List Volumes

    Gets an array of available volumes under the parent catalog and schema. There is no guarantee of a specific ordering of the elements in the array. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog\_name** | **String**| The identifier of the catalog | [default to null] |
| **schema\_name** | **String**| The identifier of the schema | [default to null] |
| **max\_results** | **Integer**| Maximum number of volumes to return (page length).  If not set, the page length is set to a server configured value. - when set to a value greater than 0, the page length is the minimum of this value and a server configured value; - when set to 0, the page length is set to a server configured value; - when set to a value less than 0, an invalid parameter error is returned;  Note: this parameter controls only the maximum number of volumes to return. The actual number of volumes returned in a page may be smaller than this value, including 0, even if there are more pages.   | [optional] [default to null] |
| **page\_token** | **String**| Opaque token returned by a previous request. It must be included in the request to retrieve the next page of results (pagination). | [optional] [default to null] |

### Return type

[**ListVolumesResponseContent**](../Models/ListVolumesResponseContent.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

<a name="updateVolume"></a>
# **updateVolume**
> VolumeInfo updateVolume(name, UpdateVolumeRequestContent)

Update a Volume

    Updates the specified volume under the specified parent catalog and schema.  Currently only the name or the comment of the volume could be updated. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **name** | **String**| The three-level (fully qualified) name of the volume | [default to null] |
| **UpdateVolumeRequestContent** | [**UpdateVolumeRequestContent**](../Models/UpdateVolumeRequestContent.md)|  | [optional] |

### Return type

[**VolumeInfo**](../Models/VolumeInfo.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

