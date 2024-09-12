# ModelVersionInfo
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **model\_name** | **String** | The name of the parent registered model of the model version, relative to parent schema | [optional] [default to null] |
| **catalog\_name** | **String** | The name of the catalog containing the model version | [optional] [default to null] |
| **schema\_name** | **String** | The name of the schema containing the model version, relative to parent catalog | [optional] [default to null] |
| **version** | **Long** | Integer model version number, used to reference the model version in API requests. | [optional] [default to null] |
| **source** | **String** | URI indicating the location of the source artifacts (files) for the model version | [optional] [default to null] |
| **run\_id** | **String** | The run id used by the ML package that generated the model. | [optional] [default to null] |
| **status** | [**ModelVersionStatus**](ModelVersionStatus.md) |  | [optional] [default to null] |
| **storage\_location** | **String** | The storage location on the cloud under which model version data files are stored | [optional] [default to null] |
| **comment** | **String** | The comment attached to the model version | [optional] [default to null] |
| **created\_at** | **Long** | Time at which this model version was created, in epoch milliseconds. | [optional] [default to null] |
| **created\_by** | **String** | The identifier of the user who created the model version | [optional] [default to null] |
| **updated\_at** | **Long** | Time at which this model version was last modified, in epoch milliseconds. | [optional] [default to null] |
| **updated\_by** | **String** | The identifier of the user who updated the model version last time | [optional] [default to null] |
| **id** | **String** | Unique identifier for the model version. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

