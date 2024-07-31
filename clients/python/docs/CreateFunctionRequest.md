# CreateFunctionRequest


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**function_info** | [**CreateFunction**](CreateFunction.md) |  | 

## Example

```python
from unitycatalog.models.create_function_request import CreateFunctionRequest

# TODO update the JSON string below
json = "{}"
# create an instance of CreateFunctionRequest from a JSON string
create_function_request_instance = CreateFunctionRequest.from_json(json)
# print the JSON string representation of the object
print(CreateFunctionRequest.to_json())

# convert the object into a dict
create_function_request_dict = create_function_request_instance.to_dict()
# create an instance of CreateFunctionRequest from a dict
create_function_request_from_dict = CreateFunctionRequest.from_dict(create_function_request_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


