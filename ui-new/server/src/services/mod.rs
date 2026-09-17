use buffa::EnumValue;
use connectrpc::{ConnectError, Router};
use serde_json::{json, Map, Value};
use std::sync::Arc;

use crate::proto::uc::v1::__buffa::view::{
    ColumnInputView, DependencyView, PageRequestView, PropertiesView, SchemaObjectRefView,
};
use crate::proto::uc::v1::{
    dependency, CatalogServiceExt, ColumnTypeName, DataSourceFormat, FunctionServiceExt,
    ModelServiceExt, SchemaServiceExt, TableServiceExt, UnityProxyServiceExt, ViewServiceExt,
    VolumeServiceExt, VolumeType,
};
use crate::upstream::Upstream;
use crate::AppState;

mod catalog;
mod function;
mod model;
mod schema;
mod table;
mod view;
mod volume;

pub(crate) struct UiRpc {
    pub(super) state: AppState,
    pub(super) upstream: Upstream,
}

impl UiRpc {
    fn new(state: AppState) -> Self {
        Self {
            upstream: Upstream::new(state.clone()),
            state,
        }
    }
}

pub(crate) fn router(state: AppState) -> Router {
    let service = Arc::new(UiRpc::new(state));
    let router = CatalogServiceExt::register(service.clone(), Router::new());
    let router = SchemaServiceExt::register(service.clone(), router);
    let router = TableServiceExt::register(service.clone(), router);
    let router = VolumeServiceExt::register(service.clone(), router);
    let router = FunctionServiceExt::register(service.clone(), router);
    let router = ModelServiceExt::register(service.clone(), router);
    let router = ViewServiceExt::register(service.clone(), router);
    UnityProxyServiceExt::register(service, router)
}

fn page_query(page: &PageRequestView<'_>) -> Vec<(&'static str, String)> {
    let mut query = Vec::new();
    if let Some(max_results) = page.max_results {
        query.push(("max_results", max_results.to_string()));
    }
    if !page.page_token.is_empty() {
        query.push(("page_token", page.page_token.to_string()));
    }
    query
}

fn object_full_name(reference: &SchemaObjectRefView<'_>) -> String {
    format!(
        "{}.{}.{}",
        reference.catalog_name, reference.schema_name, reference.name
    )
}

fn properties_value(properties: &PropertiesView<'_>) -> Value {
    Value::Object(
        properties
            .values
            .iter()
            .map(|(key, value)| (key.to_string(), json!(value)))
            .collect(),
    )
}

fn data_source_format(value: EnumValue<DataSourceFormat>) -> Result<&'static str, ConnectError> {
    match value.as_known() {
        Some(DataSourceFormat::Delta) => Ok("DELTA"),
        Some(DataSourceFormat::Iceberg) => Ok("ICEBERG"),
        Some(DataSourceFormat::Csv) => Ok("CSV"),
        Some(DataSourceFormat::Json) => Ok("JSON"),
        Some(DataSourceFormat::Avro) => Ok("AVRO"),
        Some(DataSourceFormat::Parquet) => Ok("PARQUET"),
        Some(DataSourceFormat::Orc) => Ok("ORC"),
        Some(DataSourceFormat::Text) => Ok("TEXT"),
        _ => Err(ConnectError::invalid_argument(
            "Unsupported table data source format",
        )),
    }
}

fn volume_type(value: EnumValue<VolumeType>) -> Result<&'static str, ConnectError> {
    match value.as_known() {
        Some(VolumeType::Managed) => Ok("MANAGED"),
        Some(VolumeType::External) => Ok("EXTERNAL"),
        _ => Err(ConnectError::invalid_argument("Unsupported volume type")),
    }
}

#[derive(Clone, Copy)]
struct ScalarType {
    name: &'static str,
    text: &'static str,
    spark_json: &'static str,
}

fn scalar_type(value: EnumValue<ColumnTypeName>) -> Result<ScalarType, ConnectError> {
    let scalar = match value.as_known() {
        Some(ColumnTypeName::Boolean) => ScalarType {
            name: "BOOLEAN",
            text: "boolean",
            spark_json: "boolean",
        },
        Some(ColumnTypeName::Byte) => ScalarType {
            name: "BYTE",
            text: "tinyint",
            spark_json: "byte",
        },
        Some(ColumnTypeName::Short) => ScalarType {
            name: "SHORT",
            text: "smallint",
            spark_json: "short",
        },
        Some(ColumnTypeName::Int) => ScalarType {
            name: "INT",
            text: "int",
            spark_json: "integer",
        },
        Some(ColumnTypeName::Long) => ScalarType {
            name: "LONG",
            text: "bigint",
            spark_json: "long",
        },
        Some(ColumnTypeName::Float) => ScalarType {
            name: "FLOAT",
            text: "float",
            spark_json: "float",
        },
        Some(ColumnTypeName::Double) => ScalarType {
            name: "DOUBLE",
            text: "double",
            spark_json: "double",
        },
        Some(ColumnTypeName::Date) => ScalarType {
            name: "DATE",
            text: "date",
            spark_json: "date",
        },
        Some(ColumnTypeName::Timestamp) => ScalarType {
            name: "TIMESTAMP",
            text: "timestamp",
            spark_json: "timestamp",
        },
        Some(ColumnTypeName::TimestampNtz) => ScalarType {
            name: "TIMESTAMP_NTZ",
            text: "timestamp_ntz",
            spark_json: "timestamp_ntz",
        },
        Some(ColumnTypeName::String) => ScalarType {
            name: "STRING",
            text: "string",
            spark_json: "string",
        },
        Some(ColumnTypeName::Binary) => ScalarType {
            name: "BINARY",
            text: "binary",
            spark_json: "binary",
        },
        Some(ColumnTypeName::Variant) => ScalarType {
            name: "VARIANT",
            text: "variant",
            spark_json: "variant",
        },
        _ => {
            return Err(ConnectError::invalid_argument(
                "Unsupported scalar column type",
            ));
        }
    };
    Ok(scalar)
}

fn column_value(
    column: &ColumnInputView<'_>,
    position: usize,
    mut metadata: Map<String, Value>,
) -> Result<Value, ConnectError> {
    let data_type = scalar_type(column.type_name)?;
    if let Some(comment) = &column.comment {
        metadata.insert("comment".to_string(), json!(comment));
    }
    let nullable = column.nullable.unwrap_or(true);
    let type_json = json!({
        "name": column.name,
        "type": data_type.spark_json,
        "nullable": nullable,
        "metadata": metadata,
    })
    .to_string();
    Ok(json!({
        "name": column.name,
        "type_text": data_type.text,
        "type_json": type_json,
        "type_name": data_type.name,
        "position": position,
        "comment": column.comment,
        "nullable": nullable,
    }))
}

fn dependencies_value(dependencies: &buffa::RepeatedView<'_, DependencyView<'_>>) -> Value {
    let dependencies = dependencies
        .iter()
        .filter_map(|dependency| match dependency.target.as_ref() {
            Some(dependency::TargetView::TableFullName(name)) => {
                Some(json!({ "table": { "table_full_name": name } }))
            }
            Some(dependency::TargetView::FunctionFullName(name)) => {
                Some(json!({ "function": { "function_full_name": name } }))
            }
            None => None,
        })
        .collect::<Vec<_>>();
    json!({ "dependencies": dependencies })
}
