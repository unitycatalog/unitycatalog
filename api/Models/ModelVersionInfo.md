# ModelVersionInfo
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **model\_name** | **String** | optional | The name of the parent registered model of the model version, relative to parent schema | |
| **catalog\_name** | **String** | optional | The name of the catalog containing the model version | |
| **schema\_name** | **String** | optional | The name of the schema containing the model version, relative to parent catalog | |
| **version** | **Long** | optional | Integer model version number, used to reference the model version in API requests. | |
| **source** | **String** | optional | URI indicating the location of the source artifacts (files) for the model version | |
| **run\_id** | **String** | optional | The run id used by the ML package that generated the model. | |
| **status** | [**ModelVersionStatus**](ModelVersionStatus.md) | optional |  | |
| **storage\_location** | **String** | optional | The storage location on the cloud under which model version data files are stored | |
| **comment** | **String** | optional | The comment attached to the model version | |
| **created\_at** | **Long** | optional | Time at which this model version was created, in epoch milliseconds. | |
| **created\_by** | **String** | optional | The identifier of the user who created the model version | |
| **updated\_at** | **Long** | optional | Time at which this model version was last modified, in epoch milliseconds. | |
| **updated\_by** | **String** | optional | The identifier of the user who updated the model version last time | |
| **id** | **String** | optional | Unique identifier for the model version. | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

