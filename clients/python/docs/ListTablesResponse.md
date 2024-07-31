# ListTablesResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**tables** | [**List[TableInfo]**](TableInfo.md) | An array of table information objects. | [optional] 
**next_page_token** | **str** | Opaque token to retrieve the next page of results. Absent if there are no more pages. __page_token__ should be set to this value for the next request (for the next page of results).  | [optional] 

## Example

```python
from unitycatalog.models.list_tables_response import ListTablesResponse

# TODO update the JSON string below
json = "{}"
# create an instance of ListTablesResponse from a JSON string
list_tables_response_instance = ListTablesResponse.from_json(json)
# print the JSON string representation of the object
print(ListTablesResponse.to_json())

# convert the object into a dict
list_tables_response_dict = list_tables_response_instance.to_dict()
# create an instance of ListTablesResponse from a dict
list_tables_response_from_dict = ListTablesResponse.from_dict(list_tables_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


