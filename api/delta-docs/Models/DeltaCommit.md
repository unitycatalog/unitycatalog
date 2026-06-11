# DeltaCommit
## Properties

| Name | Type | Description | Notes |
|------------ | ------------- | ------------- | -------------|
| **version** | **Long** | Commit version | [default to null] |
| **timestamp** | **Long** | In-commit timestamp, in epoch milliseconds | [default to null] |
| **file-name** | **String** | UUID-based name of the staged commit file under _delta_log/_staged_commits/. Written by the committing client and returned verbatim to readers, which read the file at that path; when the same version is also published as _delta_log/&lt;v&gt;.json, this catalog-named file is the authoritative copy.  | [default to null] |
| **file-size** | **Long** | Commit file size in bytes | [default to null] |
| **file-modification-timestamp** | **Long** | File modification timestamp, in epoch milliseconds | [default to null] |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)

