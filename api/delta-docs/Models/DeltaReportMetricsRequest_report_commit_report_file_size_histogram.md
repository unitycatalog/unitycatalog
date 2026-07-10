# DeltaReportMetricsRequest_report_commit_report_file_size_histogram
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **sorted-bin-boundaries** | **List** | Sorted bin boundaries. Each element is the start of a bin (inclusive) and the next element is the end (exclusive). The first element must be 0.  | [default to null] |
| **file-counts** | **List** | Count of files in each bin. Length must match sorted-bin-boundaries. | [default to null] |
| **total-bytes** | **List** | Total bytes in each bin. Length must match sorted-bin-boundaries. | [default to null] |
| **commit-version** | **Long** | The commit version this histogram is for | [optional] [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

