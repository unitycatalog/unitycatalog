# DeltaIdentitySequencesApi

All URIs are relative to *https://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createIdentitySequences**](DeltaIdentitySequencesApi.md#createIdentitySequences) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/identities | Create (or idempotently get) one or more monotonic identity sequences under a table. WARNING: This API is experimental and may change in future versions.  |
| [**dropIdentitySequences**](DeltaIdentitySequencesApi.md#dropIdentitySequences) | **DELETE** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/identities | Drop one or more identity sequences. WARNING: This API is experimental and may change in future versions.  |
| [**reserveIdentityRanges**](DeltaIdentitySequencesApi.md#reserveIdentityRanges) | **POST** /delta/v1/catalogs/{catalog}/schemas/{schema}/tables/{table}/identities/reserve | Reserve a contiguous range of identity values from one or more sequences. WARNING: This API is experimental and may change in future versions.  |


<a name="createIdentitySequences"></a>
# **createIdentitySequences**
> createIdentitySequences(catalog, schema, table, DeltaCreateIdentitySequences)

Create (or idempotently get) one or more monotonic identity sequences under a table. WARNING: This API is experimental and may change in future versions. 

    Create catalog-hosted monotonic sequences that back concurrent identity columns. Each sequence issues values following start + k * step for monotonically increasing k. Values are never reused and gaps from unused reserved ranges are permitted. This is a batch create-or-get: re-supplying a sequence id that already exists under the same table is a no-op when the supplied (start, step) match the stored definition, and a conflict otherwise. step must be non-zero. The batch is applied atomically: if any entry is rejected, no sequence in the request is created. Sequence ids must be unique within the request, and the request must contain at least one sequence. WARNING: This API is experimental and may change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **table** | **String**| Table name | [default to null] |
| **DeltaCreateIdentitySequences** | [**DeltaCreateIdentitySequences**](../Models/DeltaCreateIdentitySequences.md)|  | |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="dropIdentitySequences"></a>
# **dropIdentitySequences**
> DeltaDropIdentitySequencesResponse dropIdentitySequences(catalog, schema, table, DeltaDropIdentitySequences)

Drop one or more identity sequences. WARNING: This API is experimental and may change in future versions. 

    Idempotently remove sequences under a table. Dropping a sequence that does not exist (or that belongs to a different table) is a no-op that reports existed&#x3D;false for that id. Duplicate ids in the request are de-duplicated, and one result is returned per unique requested id. WARNING: This API is experimental and may change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **table** | **String**| Table name | [default to null] |
| **DeltaDropIdentitySequences** | [**DeltaDropIdentitySequences**](../Models/DeltaDropIdentitySequences.md)|  | |

### Return type

[**DeltaDropIdentitySequencesResponse**](../Models/DeltaDropIdentitySequencesResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="reserveIdentityRanges"></a>
# **reserveIdentityRanges**
> DeltaReserveIdentityRangesResponse reserveIdentityRanges(catalog, schema, table, DeltaReserveIdentityRanges)

Reserve a contiguous range of identity values from one or more sequences. WARNING: This API is experimental and may change in future versions. 

    Atomically reserve count values from each requested sequence and advance its counter. Each returned range is inclusive and never overlaps any previously issued range for that sequence. Consumers must emit range_start + i * step and must not assume range_start &lt;&#x3D; range_end, since a negative step can produce a descending range. The whole batch is atomic: if any reservation would fail (unknown sequence, step mismatch, or 64-bit overflow), no sequence in the request is advanced. Sequence ids must be unique within the request, the request must contain at least one reservation, and the returned ranges are positional with the requested reservations. WARNING: This API is experimental and may change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **catalog** | **String**| Catalog name | [default to null] |
| **schema** | **String**| Schema name | [default to null] |
| **table** | **String**| Table name | [default to null] |
| **DeltaReserveIdentityRanges** | [**DeltaReserveIdentityRanges**](../Models/DeltaReserveIdentityRanges.md)|  | |

### Return type

[**DeltaReserveIdentityRangesResponse**](../Models/DeltaReserveIdentityRangesResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

