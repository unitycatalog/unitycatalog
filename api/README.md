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
| *SchemasApi* | [**createSchema**](Apis/SchemasApi.md#createschema) | **POST** /schemas | Create a schema |
*SchemasApi* | [**deleteSchema**](Apis/SchemasApi.md#deleteschema) | **DELETE** /schemas/{full_name} | Delete a schema |
*SchemasApi* | [**getSchema**](Apis/SchemasApi.md#getschema) | **GET** /schemas/{full_name} | Get a schema |
*SchemasApi* | [**listSchemas**](Apis/SchemasApi.md#listschemas) | **GET** /schemas | List schemas |
*SchemasApi* | [**updateSchema**](Apis/SchemasApi.md#updateschema) | **PATCH** /schemas/{full_name} | Update a schema |
| *TablesApi* | [**createTable**](Apis/TablesApi.md#createtable) | **POST** /tables | Create a table. WARNING: This API is experimental and will change in future versions.  |
*TablesApi* | [**deleteTable**](Apis/TablesApi.md#deletetable) | **DELETE** /tables/{full_name} | Delete a table |
*TablesApi* | [**getTable**](Apis/TablesApi.md#gettable) | **GET** /tables/{full_name} | Get a table |
*TablesApi* | [**listTables**](Apis/TablesApi.md#listtables) | **GET** /tables | List tables |
| *TemporaryTableCredentialsApi* | [**generateTemporaryTableCredentials**](Apis/TemporaryTableCredentialsApi.md#generatetemporarytablecredentials) | **POST** /temporary-table-credentials | Generate temporary table credentials. |
| *TemporaryVolumeCredentialsApi* | [**generateTemporaryVolumeCredentials**](Apis/TemporaryVolumeCredentialsApi.md#generatetemporaryvolumecredentials) | **POST** /temporary-volume-credentials | Generate temporary volume credentials. |
| *VolumesApi* | [**createVolume**](Apis/VolumesApi.md#createvolume) | **POST** /volumes | Create a Volume |
*VolumesApi* | [**deleteVolume**](Apis/VolumesApi.md#deletevolume) | **DELETE** /volumes/{name} | Delete a Volume |
*VolumesApi* | [**getVolume**](Apis/VolumesApi.md#getvolume) | **GET** /volumes/{name} | Get a Volume |
*VolumesApi* | [**listVolumes**](Apis/VolumesApi.md#listvolumes) | **GET** /volumes | List Volumes |
*VolumesApi* | [**updateVolume**](Apis/VolumesApi.md#updatevolume) | **PATCH** /volumes/{name} | Update a Volume |


<a name="documentation-for-models"></a>
## Documentation for Models

 - [AwsCredentials](./Models/AwsCredentials.md)
 - [CatalogInfo](./Models/CatalogInfo.md)
 - [ColumnInfo](./Models/ColumnInfo.md)
 - [ColumnTypeName](./Models/ColumnTypeName.md)
 - [CreateCatalog](./Models/CreateCatalog.md)
 - [CreateFunction](./Models/CreateFunction.md)
 - [CreateFunctionRequest](./Models/CreateFunctionRequest.md)
 - [CreateSchema](./Models/CreateSchema.md)
 - [CreateTable](./Models/CreateTable.md)
 - [CreateVolumeRequestContent](./Models/CreateVolumeRequestContent.md)
 - [DataSourceFormat](./Models/DataSourceFormat.md)
 - [Dependency](./Models/Dependency.md)
 - [DependencyList](./Models/DependencyList.md)
 - [FunctionDependency](./Models/FunctionDependency.md)
 - [FunctionInfo](./Models/FunctionInfo.md)
 - [FunctionParameterInfo](./Models/FunctionParameterInfo.md)
 - [FunctionParameterInfos](./Models/FunctionParameterInfos.md)
 - [FunctionParameterMode](./Models/FunctionParameterMode.md)
 - [FunctionParameterType](./Models/FunctionParameterType.md)
 - [GenerateTemporaryTableCredential](./Models/GenerateTemporaryTableCredential.md)
 - [GenerateTemporaryTableCredentialResponse](./Models/GenerateTemporaryTableCredentialResponse.md)
 - [GenerateTemporaryVolumeCredential](./Models/GenerateTemporaryVolumeCredential.md)
 - [GenerateTemporaryVolumeCredentialResponse](./Models/GenerateTemporaryVolumeCredentialResponse.md)
 - [ListCatalogsResponse](./Models/ListCatalogsResponse.md)
 - [ListFunctionsResponse](./Models/ListFunctionsResponse.md)
 - [ListSchemasResponse](./Models/ListSchemasResponse.md)
 - [ListTablesResponse](./Models/ListTablesResponse.md)
 - [ListVolumesResponseContent](./Models/ListVolumesResponseContent.md)
 - [SchemaInfo](./Models/SchemaInfo.md)
 - [TableDependency](./Models/TableDependency.md)
 - [TableInfo](./Models/TableInfo.md)
 - [TableOperation](./Models/TableOperation.md)
 - [TableType](./Models/TableType.md)
 - [UpdateCatalog](./Models/UpdateCatalog.md)
 - [UpdateSchema](./Models/UpdateSchema.md)
 - [UpdateVolumeRequestContent](./Models/UpdateVolumeRequestContent.md)
 - [VolumeInfo](./Models/VolumeInfo.md)
 - [VolumeOperation](./Models/VolumeOperation.md)
 - [VolumeType](./Models/VolumeType.md)


<a name="documentation-for-authorization"></a>
## Documentation for Authorization

All endpoints do not require authorization.
