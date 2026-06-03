# GenerateTemporaryModelVersionCredential
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **catalog\_name** | **String** | required | Catalog name for which temporary credentials are generated.  Can be obtained from models/{full_name} (get model info) API.  | |
| **schema\_name** | **String** | required | Schema name for which temporary credentials are generated.  Can be obtained from models/{full_name} (get model info) API.  | |
| **model\_name** | **String** | required | Model name for which temporary credentials are generated.  Can be obtained from models/{full_name} (get model info) API.  | |
| **version** | **Long** | required | Model version for which temporary credentials are generated.  | |
| **operation** | [**ModelVersionOperation**](ModelVersionOperation.md) | required |  | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

