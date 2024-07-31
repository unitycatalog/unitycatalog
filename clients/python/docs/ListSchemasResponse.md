# ListSchemasResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**schemas** | [**List[SchemaInfo]**](SchemaInfo.md) | An array of schema information objects. | [optional] 
**next_page_token** | **str** | Opaque token to retrieve the next page of results. Absent if there are no more pages. __page_token__ should be set to this value for the next request (for the next page of results).  | [optional] 

## Example

```python
from unitycatalog.models.list_schemas_response import ListSchemasResponse

# TODO update the JSON string below
json = "{}"
# create an instance of ListSchemasResponse from a JSON string
list_schemas_response_instance = ListSchemasResponse.from_json(json)
# print the JSON string representation of the object
print(ListSchemasResponse.to_json())

# convert the object into a dict
list_schemas_response_dict = list_schemas_response_instance.to_dict()
# create an instance of ListSchemasResponse from a dict
list_schemas_response_from_dict = ListSchemasResponse.from_dict(list_schemas_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


