# ModelVersionInfo
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **model\_name** | **String** | Name of registered model, relative to parent schema. | [optional] [default to null] |
| **catalog\_name** | **String** | Name of parent catalog. | [optional] [default to null] |
| **schema\_name** | **String** | Name of parent schema relative to its parent catalog. | [optional] [default to null] |
| **version** | **Long** | The version number of the model version. | [optional] [default to null] |
| **source** | **String** | URI indicating the location of the source model artifacts. | [optional] [default to null] |
| **run\_id** | **String** | The run id used by the ML package that generated the model. | [optional] [default to null] |
| **status** | [**ModelVersionStatus**](ModelVersionStatus.md) |  | [optional] [default to null] |
| **storage\_location** | **String** | The storage location on the cloud under which model version data files are stored. | [optional] [default to null] |
| **comment** | **String** | User-provided free-form text description. | [optional] [default to null] |
| **created\_at** | **Long** | Time at which this model version was created, in epoch milliseconds. | [optional] [default to null] |
| **created\_by** | **String** | The user that created the model version | [optional] [default to null] |
| **updated\_at** | **Long** | Time at which this model version was last modified, in epoch milliseconds. | [optional] [default to null] |
| **updated\_by** | **String** | The user that last updated the model version | [optional] [default to null] |
| **model\_version\_id** | **String** | Unique identifier for the model version. | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

