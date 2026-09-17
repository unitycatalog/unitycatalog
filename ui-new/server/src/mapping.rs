use buffa::EnumValue;
use buffa_types::google::protobuf::Timestamp;
use connectrpc::ConnectError;
use serde_json::Value;

use crate::proto::uc::v1::*;

type MappingResult<T> = Result<T, ConnectError>;

pub(crate) fn catalog_summary(value: &Value) -> MappingResult<CatalogSummary> {
    Ok(CatalogSummary {
        name: required_string(value, "name")?,
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    })
}

pub(crate) fn catalog_info(value: &Value) -> MappingResult<CatalogInfo> {
    Ok(CatalogInfo {
        name: required_string(value, "name")?,
        comment: string(value, "comment"),
        properties: properties(value).into(),
        audit: audit(value).into(),
        id: first_string(value, &["id", "catalog_id"]),
        storage_root: string(value, "storage_root"),
        storage_location: string(value, "storage_location"),
        ..Default::default()
    })
}

pub(crate) fn schema_summary(value: &Value) -> MappingResult<SchemaSummary> {
    let catalog_name = required_string(value, "catalog_name")?;
    let name = required_string(value, "name")?;
    Ok(SchemaSummary {
        full_name: schema_full_name(value, &catalog_name, &name),
        catalog_name,
        name,
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    })
}

pub(crate) fn schema_info(value: &Value) -> MappingResult<SchemaInfo> {
    let catalog_name = required_string(value, "catalog_name")?;
    let name = required_string(value, "name")?;
    Ok(SchemaInfo {
        full_name: schema_full_name(value, &catalog_name, &name),
        catalog_name,
        name,
        comment: string(value, "comment"),
        properties: properties(value).into(),
        audit: audit(value).into(),
        id: first_string(value, &["id", "schema_id"]),
        storage_root: string(value, "storage_root"),
        storage_location: string(value, "storage_location"),
        ..Default::default()
    })
}

pub(crate) fn table_summary(value: &Value) -> MappingResult<TableSummary> {
    let (catalog_name, schema_name, name) = object_identity(value, "name")?;
    Ok(TableSummary {
        full_name: object_full_name(value, &catalog_name, &schema_name, &name),
        catalog_name,
        schema_name,
        name,
        table_type: table_type(value.get("table_type").and_then(Value::as_str)),
        data_source_format: data_source_format(
            value.get("data_source_format").and_then(Value::as_str),
        ),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    })
}

pub(crate) fn table_info(value: &Value) -> MappingResult<TableInfo> {
    let (catalog_name, schema_name, name) = object_identity(value, "name")?;
    Ok(TableInfo {
        full_name: object_full_name(value, &catalog_name, &schema_name, &name),
        catalog_name,
        schema_name,
        name,
        table_type: table_type(value.get("table_type").and_then(Value::as_str)),
        data_source_format: data_source_format(
            value.get("data_source_format").and_then(Value::as_str),
        ),
        columns: array(value, "columns")
            .iter()
            .map(column_info)
            .collect::<MappingResult<_>>()?,
        storage_location: string(value, "storage_location"),
        comment: string(value, "comment"),
        properties: properties(value).into(),
        audit: audit(value).into(),
        id: first_string(value, &["id", "table_id"]),
        ..Default::default()
    })
}

pub(crate) fn metric_view_summary(value: &Value) -> MappingResult<MetricViewSummary> {
    let (catalog_name, schema_name, name) = object_identity(value, "name")?;
    Ok(MetricViewSummary {
        full_name: object_full_name(value, &catalog_name, &schema_name, &name),
        catalog_name,
        schema_name,
        name,
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    })
}

pub(crate) fn metric_view_info(value: &Value) -> MappingResult<MetricViewInfo> {
    let (catalog_name, schema_name, name) = object_identity(value, "name")?;
    Ok(MetricViewInfo {
        full_name: object_full_name(value, &catalog_name, &schema_name, &name),
        catalog_name,
        schema_name,
        name,
        columns: array(value, "columns")
            .iter()
            .map(column_info)
            .collect::<MappingResult<_>>()?,
        view_definition: string(value, "view_definition"),
        comment: string(value, "comment"),
        properties: properties(value).into(),
        audit: audit(value).into(),
        id: first_string(value, &["id", "table_id"]),
        ..Default::default()
    })
}

pub(crate) fn volume_summary(value: &Value) -> MappingResult<VolumeSummary> {
    let (catalog_name, schema_name, name) = object_identity(value, "name")?;
    Ok(VolumeSummary {
        full_name: object_full_name(value, &catalog_name, &schema_name, &name),
        catalog_name,
        schema_name,
        name,
        volume_type: volume_type(value.get("volume_type").and_then(Value::as_str)),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    })
}

pub(crate) fn volume_info(value: &Value) -> MappingResult<VolumeInfo> {
    let (catalog_name, schema_name, name) = object_identity(value, "name")?;
    Ok(VolumeInfo {
        full_name: object_full_name(value, &catalog_name, &schema_name, &name),
        catalog_name,
        schema_name,
        name,
        volume_type: volume_type(value.get("volume_type").and_then(Value::as_str)),
        storage_location: string(value, "storage_location"),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        id: first_string(value, &["id", "volume_id"]),
        ..Default::default()
    })
}

pub(crate) fn function_summary(value: &Value) -> MappingResult<FunctionSummary> {
    let (catalog_name, schema_name, name) = object_identity(value, "name")?;
    Ok(FunctionSummary {
        full_name: object_full_name(value, &catalog_name, &schema_name, &name),
        catalog_name,
        schema_name,
        name,
        data_type: column_type(value.get("data_type").and_then(Value::as_str)),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    })
}

pub(crate) fn function_info(value: &Value) -> MappingResult<FunctionInfo> {
    let (catalog_name, schema_name, name) = object_identity(value, "name")?;
    Ok(FunctionInfo {
        full_name: object_full_name(value, &catalog_name, &schema_name, &name),
        catalog_name,
        schema_name,
        name,
        input_parameters: parameters(value.get("input_params"))?,
        data_type: column_type(value.get("data_type").and_then(Value::as_str)),
        full_data_type: string(value, "full_data_type"),
        routine_definition: string(value, "routine_definition"),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        id: first_string(value, &["id", "function_id"]),
        ..Default::default()
    })
}

pub(crate) fn model_summary(value: &Value) -> MappingResult<RegisteredModelSummary> {
    let (catalog_name, schema_name, name) = object_identity(value, "name")?;
    Ok(RegisteredModelSummary {
        full_name: object_full_name(value, &catalog_name, &schema_name, &name),
        catalog_name,
        schema_name,
        name,
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    })
}

pub(crate) fn model_info(value: &Value) -> MappingResult<RegisteredModelInfo> {
    let (catalog_name, schema_name, name) = object_identity(value, "name")?;
    Ok(RegisteredModelInfo {
        full_name: object_full_name(value, &catalog_name, &schema_name, &name),
        catalog_name,
        schema_name,
        name,
        storage_location: string(value, "storage_location"),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        id: first_string(value, &["id", "registered_model_id"]),
        ..Default::default()
    })
}

pub(crate) fn model_version_info(value: &Value) -> MappingResult<ModelVersionInfo> {
    let version = int64(value, "version")
        .filter(|version| *version > 0)
        .ok_or_else(|| invalid_response("version must be a positive integer"))?;
    Ok(ModelVersionInfo {
        catalog_name: required_string(value, "catalog_name")?,
        schema_name: required_string(value, "schema_name")?,
        model_name: first_required_string(value, &["model_name", "name"])?,
        version,
        source: string(value, "source"),
        run_id: string(value, "run_id"),
        status: model_version_status(value.get("status").and_then(Value::as_str)),
        storage_location: string(value, "storage_location"),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        id: first_string(value, &["id", "model_version_id"]),
        ..Default::default()
    })
}

pub(crate) fn page(value: &Value) -> PageResponse {
    PageResponse {
        next_page_token: string(value, "next_page_token"),
        ..Default::default()
    }
}

pub(crate) fn required_array<'a>(value: &'a Value, key: &str) -> Result<&'a [Value], ConnectError> {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .ok_or_else(|| ConnectError::internal("Invalid response from Unity Catalog server"))
}

fn audit(value: &Value) -> AuditInfo {
    AuditInfo {
        owner: string(value, "owner"),
        created_at: timestamp(int64(value, "created_at")).into(),
        created_by: string(value, "created_by"),
        updated_at: timestamp(int64(value, "updated_at")).into(),
        updated_by: string(value, "updated_by"),
        ..Default::default()
    }
}

fn timestamp(milliseconds: Option<i64>) -> Option<Timestamp> {
    milliseconds.map(|value| Timestamp {
        seconds: value.div_euclid(1000),
        nanos: (value.rem_euclid(1000) * 1_000_000) as i32,
        ..Default::default()
    })
}

fn schema_full_name(value: &Value, catalog_name: &str, name: &str) -> String {
    let full_name = string(value, "full_name");
    if !full_name.is_empty() {
        return full_name;
    }
    format!("{catalog_name}.{name}")
}

fn object_full_name(value: &Value, catalog_name: &str, schema_name: &str, name: &str) -> String {
    let full_name = string(value, "full_name");
    if !full_name.is_empty() {
        return full_name;
    }
    format!("{catalog_name}.{schema_name}.{name}")
}

fn properties(value: &Value) -> Properties {
    let values = value
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| {
            properties
                .iter()
                .filter_map(|(key, value)| {
                    value.as_str().map(|value| (key.clone(), value.to_string()))
                })
                .collect::<buffa::__private::HashMap<_, _>>()
        })
        .unwrap_or_default();
    Properties {
        values,
        ..Default::default()
    }
}

fn column_info(value: &Value) -> MappingResult<ColumnInfo> {
    Ok(ColumnInfo {
        name: required_string(value, "name")?,
        type_text: optional_string(value, "type_text"),
        type_name: column_type(value.get("type_name").and_then(Value::as_str)),
        position: int32(value, "position"),
        comment: optional_string(value, "comment"),
        nullable: optional_bool(value, "nullable"),
        ..Default::default()
    })
}

fn parameters(value: Option<&Value>) -> MappingResult<Vec<FunctionParameterInfo>> {
    value
        .and_then(|value| value.get("parameters"))
        .and_then(Value::as_array)
        .map(|parameters| parameters.iter().map(parameter).collect())
        .unwrap_or_else(|| Ok(Vec::new()))
}

fn parameter(value: &Value) -> MappingResult<FunctionParameterInfo> {
    Ok(FunctionParameterInfo {
        name: required_string(value, "name")?,
        type_text: string(value, "type_text"),
        type_name: column_type(value.get("type_name").and_then(Value::as_str)),
        position: int32(value, "position").unwrap_or_default(),
        comment: string(value, "comment"),
        ..Default::default()
    })
}

fn table_type(value: Option<&str>) -> EnumValue<TableType> {
    match value {
        Some("MANAGED") => TableType::Managed,
        Some("EXTERNAL") => TableType::External,
        Some("STREAMING_TABLE") => TableType::StreamingTable,
        Some("MATERIALIZED_VIEW") => TableType::MaterializedView,
        Some("METRIC_VIEW") => TableType::MetricView,
        Some("VIEW") => TableType::View,
        _ => TableType::Unspecified,
    }
    .into()
}

fn data_source_format(value: Option<&str>) -> EnumValue<DataSourceFormat> {
    match value {
        Some("DELTA") => DataSourceFormat::Delta,
        Some("ICEBERG") => DataSourceFormat::Iceberg,
        Some("CSV") => DataSourceFormat::Csv,
        Some("JSON") => DataSourceFormat::Json,
        Some("AVRO") => DataSourceFormat::Avro,
        Some("PARQUET") => DataSourceFormat::Parquet,
        Some("ORC") => DataSourceFormat::Orc,
        Some("TEXT") => DataSourceFormat::Text,
        _ => DataSourceFormat::Unspecified,
    }
    .into()
}

fn volume_type(value: Option<&str>) -> EnumValue<VolumeType> {
    match value {
        Some("MANAGED") => VolumeType::Managed,
        Some("EXTERNAL") => VolumeType::External,
        _ => VolumeType::Unspecified,
    }
    .into()
}

fn model_version_status(value: Option<&str>) -> EnumValue<ModelVersionStatus> {
    match value {
        Some("UNKNOWN" | "MODEL_VERSION_STATUS_UNKNOWN") => ModelVersionStatus::Unknown,
        Some("PENDING_REGISTRATION") => ModelVersionStatus::PendingRegistration,
        Some("FAILED_REGISTRATION") => ModelVersionStatus::FailedRegistration,
        Some("READY") => ModelVersionStatus::Ready,
        _ => ModelVersionStatus::Unspecified,
    }
    .into()
}

fn column_type(value: Option<&str>) -> EnumValue<ColumnTypeName> {
    match value {
        Some("BOOLEAN") => ColumnTypeName::Boolean,
        Some("BYTE") => ColumnTypeName::Byte,
        Some("SHORT") => ColumnTypeName::Short,
        Some("INT") => ColumnTypeName::Int,
        Some("LONG") => ColumnTypeName::Long,
        Some("FLOAT") => ColumnTypeName::Float,
        Some("DOUBLE") => ColumnTypeName::Double,
        Some("DATE") => ColumnTypeName::Date,
        Some("TIMESTAMP") => ColumnTypeName::Timestamp,
        Some("TIMESTAMP_NTZ") => ColumnTypeName::TimestampNtz,
        Some("STRING") => ColumnTypeName::String,
        Some("BINARY") => ColumnTypeName::Binary,
        Some("DECIMAL") => ColumnTypeName::Decimal,
        Some("INTERVAL") => ColumnTypeName::Interval,
        Some("ARRAY") => ColumnTypeName::Array,
        Some("STRUCT") => ColumnTypeName::Struct,
        Some("MAP") => ColumnTypeName::Map,
        Some("VARIANT") => ColumnTypeName::Variant,
        Some("CHAR") => ColumnTypeName::Char,
        Some("NULL") => ColumnTypeName::Null,
        Some("USER_DEFINED_TYPE") => ColumnTypeName::UserDefinedType,
        Some("TABLE_TYPE") => ColumnTypeName::TableType,
        _ => ColumnTypeName::Unspecified,
    }
    .into()
}

fn array<'a>(value: &'a Value, key: &str) -> &'a [Value] {
    value
        .get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn optional_bool(value: &Value, key: &str) -> Option<bool> {
    value.get(key).and_then(Value::as_bool)
}

fn int32(value: &Value, key: &str) -> Option<i32> {
    int64(value, key).and_then(|value| i32::try_from(value).ok())
}

fn int64(value: &Value, key: &str) -> Option<i64> {
    value.get(key).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
    })
}

fn optional_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn string(value: &Value, key: &str) -> String {
    optional_string(value, key).unwrap_or_default()
}

fn required_string(value: &Value, key: &str) -> MappingResult<String> {
    optional_string(value, key)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| invalid_response(&format!("missing or invalid {key}")))
}

fn first_string(value: &Value, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| optional_string(value, key))
        .unwrap_or_default()
}

fn first_required_string(value: &Value, keys: &[&str]) -> MappingResult<String> {
    keys.iter()
        .find_map(|key| optional_string(value, key).filter(|value| !value.is_empty()))
        .ok_or_else(|| invalid_response(&format!("missing or invalid {}", keys.join("/"))))
}

fn object_identity(value: &Value, name_key: &str) -> MappingResult<(String, String, String)> {
    Ok((
        required_string(value, "catalog_name")?,
        required_string(value, "schema_name")?,
        required_string(value, name_key)?,
    ))
}

fn invalid_response(detail: &str) -> ConnectError {
    ConnectError::internal(format!(
        "Invalid response from Unity Catalog server: {detail}"
    ))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn rejects_missing_object_identity() {
        let error = table_info(&json!({
            "catalog_name": "catalog",
            "schema_name": "schema"
        }))
        .unwrap_err();

        assert!(error
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("missing or invalid name"));
    }

    #[test]
    fn rejects_non_positive_model_version() {
        let error = model_version_info(&json!({
            "catalog_name": "catalog",
            "schema_name": "schema",
            "model_name": "model",
            "version": 0
        }))
        .unwrap_err();

        assert!(error
            .message
            .as_deref()
            .unwrap_or_default()
            .contains("version must be a positive integer"));
    }

    #[test]
    fn derives_full_name_from_valid_identity() {
        let table = table_summary(&json!({
            "catalog_name": "catalog",
            "schema_name": "schema",
            "name": "table"
        }))
        .unwrap();

        assert_eq!(table.full_name, "catalog.schema.table");
    }
}
