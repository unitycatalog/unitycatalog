# Documentation for Unity Catalog API

<a name="documentation-for-api-endpoints"></a>
## Documentation for API Endpoints

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Class | Method | HTTP request | Description |
|------------ | ------------- | ------------- | -------------|
| *CatalogsApi* | [**createCatalog**](Apis/CatalogsApi.md#createcatalog) | **POST** /catalogs | Create a Catalog |
*CatalogsApi* | [**deleteCatalog**](Apis/CatalogsApi.md#deletecatalog) | **DELETE** /catalogs/{name} | Delete a Catalog |
*CatalogsApi* | [**getCatalog**](Apis/CatalogsApi.md#getcatalog) | **GET** /catalogs/{name} | Get a Catalog |
*CatalogsApi* | [**listCatalogs**](Apis/CatalogsApi.md#listcatalogs) | **GET** /catalogs | List Catalogs |
*CatalogsApi* | [**updateCatalog**](Apis/CatalogsApi.md#updatecatalog) | **PATCH** /catalogs/{name} | Update a Catalog |
| *CredentialsApi* | [**createCredential**](Apis/CredentialsApi.md#createcredential) | **POST** /credentials | Create a Credential |
*CredentialsApi* | [**deleteCredential**](Apis/CredentialsApi.md#deletecredential) | **DELETE** /credentials/{name} | Delete a Credential |
*CredentialsApi* | [**getCredential**](Apis/CredentialsApi.md#getcredential) | **GET** /credentials/{name} | Get a Credential |
*CredentialsApi* | [**listCredentials**](Apis/CredentialsApi.md#listcredentials) | **GET** /credentials | List Credentials |
*CredentialsApi* | [**updateCredential**](Apis/CredentialsApi.md#updatecredential) | **PATCH** /credentials/{name} | Update a Credential |
| *ExternalLocationsApi* | [**createExternalLocation**](Apis/ExternalLocationsApi.md#createexternallocation) | **POST** /external-locations | Create an External Location |
*ExternalLocationsApi* | [**deleteExternalLocation**](Apis/ExternalLocationsApi.md#deleteexternallocation) | **DELETE** /external-locations/{name} | Delete an External Location |
*ExternalLocationsApi* | [**getExternalLocation**](Apis/ExternalLocationsApi.md#getexternallocation) | **GET** /external-locations/{name} | Get an External Location |
*ExternalLocationsApi* | [**listExternalLocations**](Apis/ExternalLocationsApi.md#listexternallocations) | **GET** /external-locations | List External Locations |
*ExternalLocationsApi* | [**updateExternalLocation**](Apis/ExternalLocationsApi.md#updateexternallocation) | **PATCH** /external-locations/{name} | Update an External Location |
| *FunctionsApi* | [**createFunction**](Apis/FunctionsApi.md#createfunction) | **POST** /functions | Create a Function |
*FunctionsApi* | [**deleteFunction**](Apis/FunctionsApi.md#deletefunction) | **DELETE** /functions/{name} | Delete a Function |
*FunctionsApi* | [**getFunction**](Apis/FunctionsApi.md#getfunction) | **GET** /functions/{name} | Get a Function |
*FunctionsApi* | [**listFunctions**](Apis/FunctionsApi.md#listfunctions) | **GET** /functions | List Functions |
| *GrantsApi* | [**get**](Apis/GrantsApi.md#get) | **GET** /permissions/{securable_type}/{full_name} | Get Permissions |
*GrantsApi* | [**update**](Apis/GrantsApi.md#update) | **PATCH** /permissions/{securable_type}/{full_name} | Update a Permission |
| *MetastoresApi* | [**summary**](Apis/MetastoresApi.md#summary) | **GET** /metastore_summary | Get a Metastore Summary |
| *ModelVersionsApi* | [**createModelVersion**](Apis/ModelVersionsApi.md#createmodelversion) | **POST** /models/versions | Create a Model Version |
*ModelVersionsApi* | [**deleteModelVersion**](Apis/ModelVersionsApi.md#deletemodelversion) | **DELETE** /models/{full_name}/versions/{version} | Delete a Model Version |
*ModelVersionsApi* | [**finalizeModelVersion**](Apis/ModelVersionsApi.md#finalizemodelversion) | **PATCH** /models/{full_name}/versions/{version}/finalize | Finalize a Model Version |
*ModelVersionsApi* | [**getModelVersion**](Apis/ModelVersionsApi.md#getmodelversion) | **GET** /models/{full_name}/versions/{version} | Get a Model Version |
*ModelVersionsApi* | [**listModelVersions**](Apis/ModelVersionsApi.md#listmodelversions) | **GET** /models/{full_name}/versions | List Model Versions of the Specified Registered Model |
*ModelVersionsApi* | [**updateModelVersion**](Apis/ModelVersionsApi.md#updatemodelversion) | **PATCH** /models/{full_name}/versions/{version} | Update a Model Version |
| *RegisteredModelsApi* | [**createRegisteredModel**](Apis/RegisteredModelsApi.md#createregisteredmodel) | **POST** /models | Create a Model |
*RegisteredModelsApi* | [**deleteRegisteredModel**](Apis/RegisteredModelsApi.md#deleteregisteredmodel) | **DELETE** /models/{full_name} | Delete a Registered Model |
*RegisteredModelsApi* | [**getRegisteredModel**](Apis/RegisteredModelsApi.md#getregisteredmodel) | **GET** /models/{full_name} | Get a Registered Model |
*RegisteredModelsApi* | [**listRegisteredModels**](Apis/RegisteredModelsApi.md#listregisteredmodels) | **GET** /models | List Models |
*RegisteredModelsApi* | [**updateRegisteredModel**](Apis/RegisteredModelsApi.md#updateregisteredmodel) | **PATCH** /models/{full_name} | Update a Registered Model |
| *SchemasApi* | [**createSchema**](Apis/SchemasApi.md#createschema) | **POST** /schemas | Create a Schema |
*SchemasApi* | [**deleteSchema**](Apis/SchemasApi.md#deleteschema) | **DELETE** /schemas/{full_name} | Delete a Schema |
*SchemasApi* | [**getSchema**](Apis/SchemasApi.md#getschema) | **GET** /schemas/{full_name} | Get a Schema |
*SchemasApi* | [**listSchemas**](Apis/SchemasApi.md#listschemas) | **GET** /schemas | List Schemas |
*SchemasApi* | [**updateSchema**](Apis/SchemasApi.md#updateschema) | **PATCH** /schemas/{full_name} | Update a Schema |
| *TablesApi* | [**createTable**](Apis/TablesApi.md#createtable) | **POST** /tables | Create a Table |
*TablesApi* | [**deleteTable**](Apis/TablesApi.md#deletetable) | **DELETE** /tables/{full_name} | Delete a Table |
*TablesApi* | [**getTable**](Apis/TablesApi.md#gettable) | **GET** /tables/{full_name} | Get a Table |
*TablesApi* | [**listTables**](Apis/TablesApi.md#listtables) | **GET** /tables | List Tables |
| *TemporaryCredentialsApi* | [**generateTemporaryModelVersionCredentials**](Apis/TemporaryCredentialsApi.md#generatetemporarymodelversioncredentials) | **POST** /temporary-model-version-credentials | Generate Temporary Model Version Credentials |
*TemporaryCredentialsApi* | [**generateTemporaryPathCredentials**](Apis/TemporaryCredentialsApi.md#generatetemporarypathcredentials) | **POST** /temporary-path-credentials | Generate Temporary Path Credentials |
*TemporaryCredentialsApi* | [**generateTemporaryTableCredentials**](Apis/TemporaryCredentialsApi.md#generatetemporarytablecredentials) | **POST** /temporary-table-credentials | Generate Temporary Table Credentials |
*TemporaryCredentialsApi* | [**generateTemporaryVolumeCredentials**](Apis/TemporaryCredentialsApi.md#generatetemporaryvolumecredentials) | **POST** /temporary-volume-credentials | Generate Temporary Volume Credentials |
| *VolumesApi* | [**createVolume**](Apis/VolumesApi.md#createvolume) | **POST** /volumes | Create a Volume |
*VolumesApi* | [**deleteVolume**](Apis/VolumesApi.md#deletevolume) | **DELETE** /volumes/{name} | Delete a Volume |
*VolumesApi* | [**getVolume**](Apis/VolumesApi.md#getvolume) | **GET** /volumes/{name} | Get a Volume |
*VolumesApi* | [**listVolumes**](Apis/VolumesApi.md#listvolumes) | **GET** /volumes | List Volumes |
*VolumesApi* | [**updateVolume**](Apis/VolumesApi.md#updatevolume) | **PATCH** /volumes/{name} | Update a Volume |


<a name="documentation-for-models"></a>
## Documentation for Models

 - [AwsCredentials](./Models/AwsCredentials.md)
 - [AwsIamRoleRequest](./Models/AwsIamRoleRequest.md)
 - [AwsIamRoleResponse](./Models/AwsIamRoleResponse.md)
 - [AzureUserDelegationSAS](./Models/AzureUserDelegationSAS.md)
 - [CatalogInfo](./Models/CatalogInfo.md)
 - [ColumnInfo](./Models/ColumnInfo.md)
 - [ColumnTypeName](./Models/ColumnTypeName.md)
 - [CreateCatalog](./Models/CreateCatalog.md)
 - [CreateCredentialRequest](./Models/CreateCredentialRequest.md)
 - [CreateExternalLocation](./Models/CreateExternalLocation.md)
 - [CreateFunction](./Models/CreateFunction.md)
 - [CreateFunctionRequest](./Models/CreateFunctionRequest.md)
 - [CreateModelVersion](./Models/CreateModelVersion.md)
 - [CreateRegisteredModel](./Models/CreateRegisteredModel.md)
 - [CreateSchema](./Models/CreateSchema.md)
 - [CreateTable](./Models/CreateTable.md)
 - [CreateVolumeRequestContent](./Models/CreateVolumeRequestContent.md)
 - [CredentialInfo](./Models/CredentialInfo.md)
 - [CredentialPurpose](./Models/CredentialPurpose.md)
 - [DataSourceFormat](./Models/DataSourceFormat.md)
 - [Dependency](./Models/Dependency.md)
 - [DependencyList](./Models/DependencyList.md)
 - [ExternalLocationInfo](./Models/ExternalLocationInfo.md)
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
 - [ListCredentialsResponse](./Models/ListCredentialsResponse.md)
 - [ListExternalLocationsResponse](./Models/ListExternalLocationsResponse.md)
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
 - [UpdateCredentialRequest](./Models/UpdateCredentialRequest.md)
 - [UpdateExternalLocation](./Models/UpdateExternalLocation.md)
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
