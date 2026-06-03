# VolumeInfo
## Properties

| Name | Type | Required | Description | Notes |
|------------ | ------------- | ------------- | ------------- | -------------|
| **catalog\_name** | **String** | optional | The name of the catalog where the schema and the volume are | |
| **schema\_name** | **String** | optional | The name of the schema where the volume is | |
| **name** | **String** | optional | The name of the volume | |
| **comment** | **String** | optional | The comment attached to the volume | |
| **owner** | **String** | optional | The identifier of the user who owns the volume | |
| **created\_at** | **Long** | optional | Time at which this volume was created, in epoch milliseconds. | |
| **created\_by** | **String** | optional | TThe identifier of the user who created the volume | |
| **updated\_at** | **Long** | optional | Time at which this volume was last modified, in epoch milliseconds. | |
| **updated\_by** | **String** | optional | The identifier of the user who updated the volume last time | |
| **volume\_id** | **String** | optional | Unique identifier of the volume | |
| **volume\_type** | [**VolumeType**](VolumeType.md) | optional |  | |
| **storage\_location** | **String** | optional | The storage location of the volume | |
| **full\_name** | **String** | optional | Full name of volume, in form of __catalog_name__.__schema_name__.__volume_name__. | |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

