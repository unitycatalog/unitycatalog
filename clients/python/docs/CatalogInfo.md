# CatalogInfo


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | Name of catalog. | [optional] 
**comment** | **str** | User-provided free-form text description. | [optional] 
**properties** | **Dict[str, str]** | A map of key-value properties attached to the securable. | [optional] 
**created_at** | **int** | Time at which this catalog was created, in epoch milliseconds. | [optional] 
**updated_at** | **int** | Time at which this catalog was last modified, in epoch milliseconds. | [optional] 
**id** | **str** | Unique identifier for the catalog. | [optional] 

## Example

```python
from unitycatalog.models.catalog_info import CatalogInfo

# TODO update the JSON string below
json = "{}"
# create an instance of CatalogInfo from a JSON string
catalog_info_instance = CatalogInfo.from_json(json)
# print the JSON string representation of the object
print(CatalogInfo.to_json())

# convert the object into a dict
catalog_info_dict = catalog_info_instance.to_dict()
# create an instance of CatalogInfo from a dict
catalog_info_from_dict = CatalogInfo.from_dict(catalog_info_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


