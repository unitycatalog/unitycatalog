# CreateCatalog


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**name** | **str** | Name of catalog. | 
**comment** | **str** | User-provided free-form text description. | [optional] 
**properties** | **Dict[str, str]** | A map of key-value properties attached to the securable. | [optional] 

## Example

```python
from unitycatalog.models.create_catalog import CreateCatalog

# TODO update the JSON string below
json = "{}"
# create an instance of CreateCatalog from a JSON string
create_catalog_instance = CreateCatalog.from_json(json)
# print the JSON string representation of the object
print(CreateCatalog.to_json())

# convert the object into a dict
create_catalog_dict = create_catalog_instance.to_dict()
# create an instance of CreateCatalog from a dict
create_catalog_from_dict = CreateCatalog.from_dict(create_catalog_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


