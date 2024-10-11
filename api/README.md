# Documentation for Unity Catalog API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Class | Method | HTTP request | Description |
|------------ | ------------- | ------------- | -------------|
| *CatalogsApi* | [**createCatalog**](Apis/CatalogsApi.md#createcatalog) | **POST** /catalogs | Create a catalog |
*CatalogsApi* | [**deleteCatalog**](Apis/CatalogsApi.md#deletecatalog) | **DELETE** /catalogs/{name} | Delete a catalog |
*CatalogsApi* | [**getCatalog**](Apis/CatalogsApi.md#getcatalog) | **GET** /catalogs/{name} | Get a catalog |
*CatalogsApi* | [**listCatalogs**](Apis/CatalogsApi.md#listcatalogs) | **GET** /catalogs | List catalogs |
*CatalogsApi* | [**updateCatalog**](Apis/CatalogsApi.md#updatecatalog) | **PATCH** /catalogs/{name} | Update a catalog |
| *FunctionsApi* | [**createFunction**](Apis/FunctionsApi.md#createfunction) | **POST** /functions | Create a function. WARNING: This API is experimental and will change in future versions.  |
*FunctionsApi* | [**deleteFunction**](Apis/FunctionsApi.md#deletefunction) | **DELETE** /functions/{name} | Delete a function |
*FunctionsApi* | [**getFunction**](Apis/FunctionsApi.md#getfunction) | **GET** /functions/{name} | Get a function |
*FunctionsApi* | [**listFunctions**](Apis/FunctionsApi.md#listfunctions) | **GET** /functions | List functions |
| *GrantsApi* | [**get**](Apis/GrantsApi.md#get) | **GET** /permissions/{securable_type}/{full_name} | Get permissions |
*GrantsApi* | [**update**](Apis/GrantsApi.md#update) | **PATCH** /permissions/{securable_type}/{full_name} | Update a permission |
| *MetastoresApi* | [**summary**](Apis/MetastoresApi.md#summary) | **GET** /metastore_summary | Get metastore summary |
| *ModelVersionsApi* | [**createModelVersion**](Apis/ModelVersionsApi.md#createmodelversion) | **POST** /models/versions | Create a model version.  |
*ModelVersionsApi* | [**deleteModelVersion**](Apis/ModelVersionsApi.md#deletemodelversion) | **DELETE** /models/{full_name}/versions/{version} | Delete a model version |
*ModelVersionsApi* | [**finalizeModelVersion**](Apis/ModelVersionsApi.md#finalizemodelversion) | **PATCH** /models/{full_name}/versions/{version}/finalize | Finalize a model version |
*ModelVersionsApi* | [**getModelVersion**](Apis/ModelVersionsApi.md#getmodelversion) | **GET** /models/{full_name}/versions/{version} | Get a model version |
*ModelVersionsApi* | [**listModelVersions**](Apis/ModelVersionsApi.md#listmodelversions) | **GET** /models/{full_name}/versions | List model versions of the specified registered model. |
*ModelVersionsApi* | [**updateModelVersion**](Apis/ModelVersionsApi.md#updatemodelversion) | **PATCH** /models/{full_name}/versions/{version} | Update a model version |
| *RegisteredModelsApi* | [**createRegisteredModel**](Apis/RegisteredModelsApi.md#createregisteredmodel) | **POST** /models | Create a model. WARNING: This API is experimental and will change in future versions.  |
*RegisteredModelsApi* | [**deleteRegisteredModel**](Apis/RegisteredModelsApi.md#deleteregisteredmodel) | **DELETE** /models/{full_name} | Delete a specified registered model. |
*RegisteredModelsApi* | [**getRegisteredModel**](Apis/RegisteredModelsApi.md#getregisteredmodel) | **GET** /models/{full_name} | Get a specified registered model |
*RegisteredModelsApi* | [**listRegisteredModels**](Apis/RegisteredModelsApi.md#listregisteredmodels) | **GET** /models | List models |
*RegisteredModelsApi* | [**updateRegisteredModel**](Apis/RegisteredModelsApi.md#updateregisteredmodel) | **PATCH** /models/{full_name} | Update a registered model |
| *SchemasApi* | [**createSchema**](Apis/SchemasApi.md#createschema) | **POST** /schemas | Create a schema |
*SchemasApi* | [**deleteSchema**](Apis/SchemasApi.md#deleteschema) | **DELETE** /schemas/{full_name} | Delete a schema |
*SchemasApi* | [**getSchema**](Apis/SchemasApi.md#getschema) | **GET** /schemas/{full_name} | Get a schema |
*SchemasApi* | [**listSchemas**](Apis/SchemasApi.md#listschemas) | **GET** /schemas | List schemas |
*SchemasApi* | [**updateSchema**](Apis/SchemasApi.md#updateschema) | **PATCH** /schemas/{full_name} | Update a schema |
| *TablesApi* | [**createTable**](Apis/TablesApi.md#createtable) | **POST** /tables | Create a table. Only external table creation is supported. WARNING: This API is experimental and will change in future versions.  |
*TablesApi* | [**deleteTable**](Apis/TablesApi.md#deletetable) | **DELETE** /tables/{full_name} | Delete a table |
*TablesApi* | [**getTable**](Apis/TablesApi.md#gettable) | **GET** /tables/{full_name} | Get a table |
*TablesApi* | [**listTables**](Apis/TablesApi.md#listtables) | **GET** /tables | List tables |
| *TemporaryCredentialsApi* | [**generateTemporaryModelVersionCredentials**](Apis/TemporaryCredentialsApi.md#generatetemporarymodelversioncredentials) | **POST** /temporary-model-version-credentials | Generate temporary model version credentials. These credentials are used by clients to write and retrieve model artifacts from the model versions external storage location. |
*TemporaryCredentialsApi* | [**generateTemporaryPathCredentials**](Apis/TemporaryCredentialsApi.md#generatetemporarypathcredentials) | **POST** /temporary-path-credentials | Generate temporary path credentials. |
*TemporaryCredentialsApi* | [**generateTemporaryTableCredentials**](Apis/TemporaryCredentialsApi.md#generatetemporarytablecredentials) | **POST** /temporary-table-credentials | Generate temporary table credentials. |
*TemporaryCredentialsApi* | [**generateTemporaryVolumeCredentials**](Apis/TemporaryCredentialsApi.md#generatetemporaryvolumecredentials) | **POST** /temporary-volume-credentials | Generate temporary volume credentials. |
| *VolumesApi* | [**createVolume**](Apis/VolumesApi.md#createvolume) | **POST** /volumes | Create a Volume |
*VolumesApi* | [**deleteVolume**](Apis/VolumesApi.md#deletevolume) | **DELETE** /volumes/{name} | Delete a Volume |
*VolumesApi* | [**getVolume**](Apis/VolumesApi.md#getvolume) | **GET** /volumes/{name} | Get a Volume |
*VolumesApi* | [**listVolumes**](Apis/VolumesApi.md#listvolumes) | **GET** /volumes | List Volumes |
*VolumesApi* | [**updateVolume**](Apis/VolumesApi.md#updatevolume) | **PATCH** /volumes/{name} | Update a Volume |


<a name="documentation-for-models"></a>
## Documentation for Models

 - [AwsCredentials](./Models/AwsCredentials.md)
 - [AzureUserDelegationSAS](./Models/AzureUserDelegationSAS.md)
 - [CatalogInfo](./Models/CatalogInfo.md)
 - [ColumnInfo](./Models/ColumnInfo.md)
 - [ColumnTypeName](./Models/ColumnTypeName.md)
 - [CreateCatalog](./Models/CreateCatalog.md)
 - [CreateFunction](./Models/CreateFunction.md)
 - [CreateFunctionRequest](./Models/CreateFunctionRequest.md)
 - [CreateModelVersion](./Models/CreateModelVersion.md)
 - [CreateRegisteredModel](./Models/CreateRegisteredModel.md)
 - [CreateSchema](./Models/CreateSchema.md)
 - [CreateTable](./Models/CreateTable.md)
 - [CreateVolumeRequestContent](./Models/CreateVolumeRequestContent.md)
 - [DataSourceFormat](./Models/DataSourceFormat.md)
 - [Dependency](./Models/Dependency.md)
 - [DependencyList](./Models/DependencyList.md)
 - [FinalizeModelVersion](./Models/FinalizeModelVersion.md)
 - [FunctionDependency](./Models/FunctionDependency.md)
 - [FunctionInfo](./Models/FunctionInfo.md)
 - [FunctionParameterInfo](./Models/FunctionParameterInfo.md)
 - [FunctionParameterInfos](./Models/FunctionParameterInfos.md)
 - [FunctionParameterMode](./Models/FunctionParameterMode.md)
 - [FunctionParameterType](./Models/FunctionParameterType.md)
 - [GcpOauthToken](./Models/GcpOauthToken.md)
 - [GenerateTemporaryModelVersionCredential](./Models/GenerateTemporaryModelVersionCredential.md)
 - [GenerateTemporaryPathCredential](./Models/GenerateTemporaryPathCredential.md)
 - [GenerateTemporaryTableCredential](./Models/GenerateTemporaryTableCredential.md)
 - [GenerateTemporaryVolumeCredential](./Models/GenerateTemporaryVolumeCredential.md)
 - [GetMetastoreSummaryResponse](./Models/GetMetastoreSummaryResponse.md)
 - [ListCatalogsResponse](./Models/ListCatalogsResponse.md)
 - [ListFunctionsResponse](./Models/ListFunctionsResponse.md)
 - [ListModelVersionsResponse](./Models/ListModelVersionsResponse.md)
 - [ListRegisteredModelsResponse](./Models/ListRegisteredModelsResponse.md)
 - [ListSchemasResponse](./Models/ListSchemasResponse.md)
 - [ListTablesResponse](./Models/ListTablesResponse.md)
 - [ListVolumesResponseContent](./Models/ListVolumesResponseContent.md)
 - [ModelVersionInfo](./Models/ModelVersionInfo.md)
 - [ModelVersionOperation](./Models/ModelVersionOperation.md)
 - [ModelVersionStatus](./Models/ModelVersionStatus.md)
 - [PathOperation](./Models/PathOperation.md)
 - [PermissionsChange](./Models/PermissionsChange.md)
 - [PermissionsList](./Models/PermissionsList.md)
 - [PrincipalType](./Models/PrincipalType.md)
 - [Privilege](./Models/Privilege.md)
 - [PrivilegeAssignment](./Models/PrivilegeAssignment.md)
 - [RegisteredModelInfo](./Models/RegisteredModelInfo.md)
 - [SchemaInfo](./Models/SchemaInfo.md)
 - [SecurableType](./Models/SecurableType.md)
 - [TableDependency](./Models/TableDependency.md)
 - [TableInfo](./Models/TableInfo.md)
 - [TableOperation](./Models/TableOperation.md)
 - [TableType](./Models/TableType.md)
 - [TemporaryCredentials](./Models/TemporaryCredentials.md)
 - [UpdateCatalog](./Models/UpdateCatalog.md)
 - [UpdateModelVersion](./Models/UpdateModelVersion.md)
 - [UpdatePermissions](./Models/UpdatePermissions.md)
 - [UpdateRegisteredModel](./Models/UpdateRegisteredModel.md)
 - [UpdateSchema](./Models/UpdateSchema.md)
 - [UpdateVolumeRequestContent](./Models/UpdateVolumeRequestContent.md)
 - [VolumeInfo](./Models/VolumeInfo.md)
 - [VolumeOperation](./Models/VolumeOperation.md)
 - [VolumeType](./Models/VolumeType.md)


<a name="documentation-for-authorization"></a>
## Documentation for Authorization

All endpoints do not require authorization.
