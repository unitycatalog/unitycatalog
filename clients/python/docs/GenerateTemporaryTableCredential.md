# GenerateTemporaryTableCredential


## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**table_id** | **str** | Table id for which temporary credentials are generated.  Can be obtained from tables/{full_name} (get table info) API.  | 
**operation** | [**TableOperation**](TableOperation.md) |  | 

## Example

```python
from unitycatalog.models.generate_temporary_table_credential import GenerateTemporaryTableCredential

# TODO update the JSON string below
json = "{}"
# create an instance of GenerateTemporaryTableCredential from a JSON string
generate_temporary_table_credential_instance = GenerateTemporaryTableCredential.from_json(json)
# print the JSON string representation of the object
print(GenerateTemporaryTableCredential.to_json())

# convert the object into a dict
generate_temporary_table_credential_dict = generate_temporary_table_credential_instance.to_dict()
# create an instance of GenerateTemporaryTableCredential from a dict
generate_temporary_table_credential_from_dict = GenerateTemporaryTableCredential.from_dict(generate_temporary_table_credential_dict)
```
[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


