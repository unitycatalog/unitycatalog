use buffa::EnumValue;
use buffa_types::google::protobuf::Timestamp;
use connectrpc::ConnectError;
use serde_json::Value;

use crate::proto::uc::v1::*;

pub(crate) fn catalog_summary(value: &Value) -> CatalogSummary {
    CatalogSummary {
        name: string(value, "name"),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    }
}

pub(crate) fn catalog_info(value: &Value) -> CatalogInfo {
    CatalogInfo {
        name: string(value, "name"),
        comment: string(value, "comment"),
        properties: properties(value).into(),
        audit: audit(value).into(),
        id: first_string(value, &["id", "catalog_id"]),
        storage_root: string(value, "storage_root"),
        storage_location: string(value, "storage_location"),
        ..Default::default()
    }
}

pub(crate) fn schema_summary(value: &Value) -> SchemaSummary {
    SchemaSummary {
        catalog_name: string(value, "catalog_name"),
        name: string(value, "name"),
        full_name: schema_full_name(value),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    }
}

pub(crate) fn schema_info(value: &Value) -> SchemaInfo {
    SchemaInfo {
        catalog_name: string(value, "catalog_name"),
        name: string(value, "name"),
        full_name: schema_full_name(value),
        comment: string(value, "comment"),
        properties: properties(value).into(),
        audit: audit(value).into(),
        id: first_string(value, &["id", "schema_id"]),
        storage_root: string(value, "storage_root"),
        storage_location: string(value, "storage_location"),
        ..Default::default()
    }
}

pub(crate) fn table_summary(value: &Value) -> TableSummary {
    TableSummary {
        catalog_name: string(value, "catalog_name"),
        schema_name: string(value, "schema_name"),
        name: string(value, "name"),
        full_name: table_full_name(value),
        table_type: table_type(value.get("table_type").and_then(Value::as_str)),
        data_source_format: data_source_format(
            value.get("data_source_format").and_then(Value::as_str),
        ),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    }
}

pub(crate) fn table_info(value: &Value) -> TableInfo {
    TableInfo {
        catalog_name: string(value, "catalog_name"),
        schema_name: string(value, "schema_name"),
        name: string(value, "name"),
        full_name: table_full_name(value),
        table_type: table_type(value.get("table_type").and_then(Value::as_str)),
        data_source_format: data_source_format(
            value.get("data_source_format").and_then(Value::as_str),
        ),
        columns: array(value, "columns").iter().map(column_info).collect(),
        storage_location: string(value, "storage_location"),
        comment: string(value, "comment"),
        properties: properties(value).into(),
        audit: audit(value).into(),
        id: first_string(value, &["id", "table_id"]),
        view_definition: string(value, "view_definition"),
        dependencies: dependencies(value.get("view_dependencies")),
        ..Default::default()
    }
}

pub(crate) fn metric_view_summary(value: &Value) -> MetricViewSummary {
    MetricViewSummary {
        catalog_name: string(value, "catalog_name"),
        schema_name: string(value, "schema_name"),
        name: string(value, "name"),
        full_name: table_full_name(value),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    }
}

pub(crate) fn metric_view_info(value: &Value) -> MetricViewInfo {
    MetricViewInfo {
        catalog_name: string(value, "catalog_name"),
        schema_name: string(value, "schema_name"),
        name: string(value, "name"),
        full_name: table_full_name(value),
        columns: array(value, "columns").iter().map(column_info).collect(),
        view_definition: string(value, "view_definition"),
        dependencies: dependencies(value.get("view_dependencies")),
        comment: string(value, "comment"),
        properties: properties(value).into(),
        audit: audit(value).into(),
        id: first_string(value, &["id", "table_id"]),
        ..Default::default()
    }
}

pub(crate) fn volume_summary(value: &Value) -> VolumeSummary {
    VolumeSummary {
        catalog_name: string(value, "catalog_name"),
        schema_name: string(value, "schema_name"),
        name: string(value, "name"),
        full_name: string(value, "full_name"),
        volume_type: volume_type(value.get("volume_type").and_then(Value::as_str)),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    }
}

pub(crate) fn volume_info(value: &Value) -> VolumeInfo {
    VolumeInfo {
        catalog_name: string(value, "catalog_name"),
        schema_name: string(value, "schema_name"),
        name: string(value, "name"),
        full_name: string(value, "full_name"),
        volume_type: volume_type(value.get("volume_type").and_then(Value::as_str)),
        storage_location: string(value, "storage_location"),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        id: first_string(value, &["id", "volume_id"]),
        ..Default::default()
    }
}

pub(crate) fn function_summary(value: &Value) -> FunctionSummary {
    FunctionSummary {
        catalog_name: string(value, "catalog_name"),
        schema_name: string(value, "schema_name"),
        name: string(value, "name"),
        full_name: string(value, "full_name"),
        data_type: column_type(value.get("data_type").and_then(Value::as_str)),
        language: function_language(value.get("routine_body").and_then(Value::as_str)),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    }
}

pub(crate) fn function_info(value: &Value) -> FunctionInfo {
    FunctionInfo {
        catalog_name: string(value, "catalog_name"),
        schema_name: string(value, "schema_name"),
        name: string(value, "name"),
        full_name: string(value, "full_name"),
        input_parameters: parameters(value.get("input_params")),
        data_type: column_type(value.get("data_type").and_then(Value::as_str)),
        full_data_type: string(value, "full_data_type"),
        language: function_language(value.get("routine_body").and_then(Value::as_str)),
        routine_definition: string(value, "routine_definition"),
        sql_data_access: sql_data_access(value.get("sql_data_access").and_then(Value::as_str)),
        is_deterministic: optional_bool(value, "is_deterministic"),
        is_null_call: optional_bool(value, "is_null_call"),
        external_language: string(value, "external_language"),
        comment: string(value, "comment"),
        properties_json: string(value, "properties"),
        audit: audit(value).into(),
        id: first_string(value, &["id", "function_id"]),
        return_parameters: parameters(value.get("return_params")),
        routine_dependencies: dependencies(value.get("routine_dependencies")),
        ..Default::default()
    }
}

pub(crate) fn model_summary(value: &Value) -> RegisteredModelSummary {
    RegisteredModelSummary {
        catalog_name: string(value, "catalog_name"),
        schema_name: string(value, "schema_name"),
        name: string(value, "name"),
        full_name: string(value, "full_name"),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        ..Default::default()
    }
}

pub(crate) fn model_info(value: &Value) -> RegisteredModelInfo {
    RegisteredModelInfo {
        catalog_name: string(value, "catalog_name"),
        schema_name: string(value, "schema_name"),
        name: string(value, "name"),
        full_name: string(value, "full_name"),
        storage_location: string(value, "storage_location"),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        id: first_string(value, &["id", "registered_model_id"]),
        ..Default::default()
    }
}

pub(crate) fn model_version_info(value: &Value) -> ModelVersionInfo {
    ModelVersionInfo {
        catalog_name: string(value, "catalog_name"),
        schema_name: string(value, "schema_name"),
        model_name: first_string(value, &["model_name", "name"]),
        version: int64(value, "version").unwrap_or_default(),
        source: string(value, "source"),
        run_id: string(value, "run_id"),
        status: model_version_status(value.get("status").and_then(Value::as_str)),
        storage_location: string(value, "storage_location"),
        comment: string(value, "comment"),
        audit: audit(value).into(),
        id: first_string(value, &["id", "model_version_id"]),
        ..Default::default()
    }
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

fn schema_full_name(value: &Value) -> String {
    let full_name = string(value, "full_name");
    if !full_name.is_empty() {
        return full_name;
    }
    [string(value, "catalog_name"), string(value, "name")].join(".")
}

fn table_full_name(value: &Value) -> String {
    let full_name = string(value, "full_name");
    if !full_name.is_empty() {
        return full_name;
    }
    [
        string(value, "catalog_name"),
        string(value, "schema_name"),
        string(value, "name"),
    ]
    .join(".")
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

fn column_info(value: &Value) -> ColumnInfo {
    ColumnInfo {
        name: string(value, "name"),
        type_text: optional_string(value, "type_text"),
        type_json: optional_string(value, "type_json"),
        type_name: column_type(value.get("type_name").and_then(Value::as_str)),
        type_precision: int32(value, "type_precision"),
        type_scale: int32(value, "type_scale"),
        type_interval_type: optional_string(value, "type_interval_type"),
        position: int32(value, "position"),
        comment: optional_string(value, "comment"),
        nullable: optional_bool(value, "nullable"),
        partition_index: int32(value, "partition_index"),
        ..Default::default()
    }
}

fn parameters(value: Option<&Value>) -> Vec<FunctionParameterInfo> {
    value
        .and_then(|value| value.get("parameters"))
        .and_then(Value::as_array)
        .map(|parameters| parameters.iter().map(parameter).collect())
        .unwrap_or_default()
}

fn parameter(value: &Value) -> FunctionParameterInfo {
    FunctionParameterInfo {
        name: string(value, "name"),
        type_text: string(value, "type_text"),
        type_json: string(value, "type_json"),
        type_name: column_type(value.get("type_name").and_then(Value::as_str)),
        position: int32(value, "position").unwrap_or_default(),
        parameter_default: string(value, "parameter_default"),
        comment: string(value, "comment"),
        type_precision: int32(value, "type_precision"),
        type_scale: int32(value, "type_scale"),
        type_interval_type: string(value, "type_interval_type"),
        parameter_mode: parameter_mode(value.get("parameter_mode").and_then(Value::as_str)),
        parameter_type: parameter_type(value.get("parameter_type").and_then(Value::as_str)),
        ..Default::default()
    }
}

fn dependencies(value: Option<&Value>) -> Vec<Dependency> {
    value
        .and_then(|value| value.get("dependencies"))
        .and_then(Value::as_array)
        .map(|dependencies| dependencies.iter().filter_map(dependency).collect())
        .unwrap_or_default()
}

fn dependency(value: &Value) -> Option<Dependency> {
    let target = if let Some(name) = value
        .get("table")
        .and_then(|table| table.get("table_full_name"))
        .and_then(Value::as_str)
    {
        dependency::Target::TableFullName(name.to_string())
    } else if let Some(name) = value
        .get("function")
        .and_then(|function| function.get("function_full_name"))
        .and_then(Value::as_str)
    {
        dependency::Target::FunctionFullName(name.to_string())
    } else {
        return None;
    };
    Some(Dependency {
        target: Some(target),
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

fn function_language(value: Option<&str>) -> EnumValue<FunctionLanguage> {
    match value {
        Some("SQL") => FunctionLanguage::Sql,
        Some("EXTERNAL") => FunctionLanguage::External,
        _ => FunctionLanguage::Unspecified,
    }
    .into()
}

fn sql_data_access(value: Option<&str>) -> EnumValue<FunctionSqlDataAccess> {
    match value {
        Some("CONTAINS_SQL") => FunctionSqlDataAccess::ContainsSql,
        Some("READS_SQL_DATA") => FunctionSqlDataAccess::ReadsSqlData,
        Some("NO_SQL") => FunctionSqlDataAccess::NoSql,
        _ => FunctionSqlDataAccess::Unspecified,
    }
    .into()
}

fn parameter_mode(value: Option<&str>) -> EnumValue<FunctionParameterMode> {
    match value {
        Some("IN") => FunctionParameterMode::In,
        _ => FunctionParameterMode::Unspecified,
    }
    .into()
}

fn parameter_type(value: Option<&str>) -> EnumValue<FunctionParameterType> {
    match value {
        Some("PARAM") => FunctionParameterType::Param,
        Some("COLUMN") => FunctionParameterType::Column,
        _ => FunctionParameterType::Unspecified,
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

fn first_string(value: &Value, keys: &[&str]) -> String {
    keys.iter()
        .find_map(|key| optional_string(value, key))
        .unwrap_or_default()
}
