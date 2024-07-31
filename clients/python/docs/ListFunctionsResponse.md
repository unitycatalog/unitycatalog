# ListFunctionsResponse


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**functions** | [**List[FunctionInfo]**](FunctionInfo.md) | An array of function information objects. | [optional] 
**next_page_token** | **str** | Opaque token to retrieve the next page of results. Absent if there are no more pages. __page_token__ should be set to this value for the next request (for the next page of results).  | [optional] 

## Example

```python
from unitycatalog.models.list_functions_response import ListFunctionsResponse

# TODO update the JSON string below
json = "{}"
# create an instance of ListFunctionsResponse from a JSON string
list_functions_response_instance = ListFunctionsResponse.from_json(json)
# print the JSON string representation of the object
print(ListFunctionsResponse.to_json())

# convert the object into a dict
list_functions_response_dict = list_functions_response_instance.to_dict()
# create an instance of ListFunctionsResponse from a dict
list_functions_response_from_dict = ListFunctionsResponse.from_dict(list_functions_response_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


