# IdentitySequencesApi

All URIs are relative to *http://localhost:8080/api/2.1/unity-catalog*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createIdentitySequences**](IdentitySequencesApi.md#createIdentitySequences) | **POST** /identity/sequence | Create (or idempotently get) one or more monotonic identity sequences under a table. WARNING: This API is experimental and may change in future versions.  |
| [**dropIdentitySequences**](IdentitySequencesApi.md#dropIdentitySequences) | **DELETE** /identity/sequence | Drop one or more identity sequences. WARNING: This API is experimental and may change in future versions.  |
| [**reserveIdentityRanges**](IdentitySequencesApi.md#reserveIdentityRanges) | **POST** /identity/sequence/reserve | Reserve a contiguous range of identity values from one or more sequences. WARNING: This API is experimental and may change in future versions.  |


<a name="createIdentitySequences"></a>
# **createIdentitySequences**
> createIdentitySequences(CreateIdentitySequences)

Create (or idempotently get) one or more monotonic identity sequences under a table. WARNING: This API is experimental and may change in future versions. 

    Create catalog-hosted monotonic sequences that back concurrent identity columns. Each sequence issues values following start + k * step for monotonically increasing k. Values are never reused and gaps from unused reserved ranges are permitted. This is a batch create-or-get: re-supplying a sequence id that already exists under the same table is a no-op when the supplied (start, step) match the stored definition, and a conflict otherwise. step must be non-zero. The batch is applied atomically: if any entry is rejected, no sequence in the request is created. Sequence ids must be unique within the request, and the request must contain at least one sequence. WARNING: This API is experimental and may change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **CreateIdentitySequences** | [**CreateIdentitySequences**](../Models/CreateIdentitySequences.md)|  | |

### Return type

null (empty response body)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

<a name="dropIdentitySequences"></a>
# **dropIdentitySequences**
> DropIdentitySequencesResponse dropIdentitySequences(DropIdentitySequences)

Drop one or more identity sequences. WARNING: This API is experimental and may change in future versions. 

    Idempotently remove sequences under a table. Dropping a sequence that does not exist (or that belongs to a different table) is a no-op that reports existed&#x3D;false for that id. Duplicate ids in the request are de-duplicated, and one result is returned per unique requested id. WARNING: This API is experimental and may change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **DropIdentitySequences** | [**DropIdentitySequences**](../Models/DropIdentitySequences.md)|  | |

### Return type

[**DropIdentitySequencesResponse**](../Models/DropIdentitySequencesResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

<a name="reserveIdentityRanges"></a>
# **reserveIdentityRanges**
> ReserveIdentityRangesResponse reserveIdentityRanges(ReserveIdentityRanges)

Reserve a contiguous range of identity values from one or more sequences. WARNING: This API is experimental and may change in future versions. 

    Atomically reserve count values from each requested sequence and advance its counter. Each returned range is inclusive and never overlaps any previously issued range for that sequence. Consumers must emit range_start + i * step and must not assume range_start &lt;&#x3D; range_end, since a negative step can produce a descending range. The whole batch is atomic: if any reservation would fail (unknown sequence, step mismatch, or 64-bit overflow), no sequence in the request is advanced. Sequence ids must be unique within the request, the request must contain at least one reservation, and the returned ranges are positional with the requested reservations. WARNING: This API is experimental and may change in future versions. 

### Parameters

|Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **ReserveIdentityRanges** | [**ReserveIdentityRanges**](../Models/ReserveIdentityRanges.md)|  | |

### Return type

[**ReserveIdentityRangesResponse**](../Models/ReserveIdentityRangesResponse.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

