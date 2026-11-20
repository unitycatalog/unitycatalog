use buffa::EnumValue;
use connectrpc::{ConnectError, RequestContext, Router};
use serde_json::{json, Map, Value};
use std::sync::Arc;

use crate::mapping;
use crate::proto::uc::v1::__buffa::view::{
    ColumnInputView, DependencyView, PageRequestView, PropertiesView, SchemaObjectRefView,
};
use crate::proto::uc::v1::{
    dependency, CatalogServiceExt, ColumnTypeName, DataSourceFormat, FunctionServiceExt,
    ModelServiceExt, SchemaServiceExt, TableServiceExt, TableType, UnityProxyServiceExt,
    ViewServiceExt, VolumeServiceExt, VolumeType,
};
use crate::upstream::{path_segment, Upstream};
use crate::AppState;

mod catalog;
mod function;
mod model;
mod schema;
mod table;
mod view;
mod volume;

pub(crate) struct UiRpc {
    pub(crate) upstream: Upstream,
}

impl UiRpc {
    fn new(state: AppState) -> Self {
        Self {
            upstream: Upstream::new(state),
        }
    }

    /// Reads the UC table resource at `full_name`. A resource of the other kind
    /// is NOT_FOUND to the typed service asking for `expected`.
    async fn get_table_resource(
        &self,
        ctx: &RequestContext,
        full_name: &str,
        expected: TableKind,
    ) -> Result<Value, ConnectError> {
        let value = self
            .upstream
            .get(ctx, &table_path(full_name), Vec::new())
            .await?;
        match kind_mismatch(&value, full_name, expected)? {
            Some(message) => Err(ConnectError::not_found(message)),
            None => Ok(value),
        }
    }

    /// Deletes the UC table resource at `full_name` only if it is `expected`.
    /// UC's delete takes just the name, so the kind is read first.
    async fn delete_table_resource(
        &self,
        ctx: &RequestContext,
        full_name: &str,
        expected: TableKind,
    ) -> Result<(), ConnectError> {
        let path = table_path(full_name);
        let value = self.upstream.get(ctx, &path, Vec::new()).await?;
        if let Some(message) = kind_mismatch(&value, full_name, expected)? {
            return Err(ConnectError::failed_precondition(format!(
                "{message}; nothing was deleted"
            )));
        }
        self.upstream.delete(ctx, &path, Vec::new()).await
    }
}

/// Tables and metric views share UC's `/tables/{full_name}` resource; the typed
/// table and view services each act only on their own kind.
#[derive(Clone, Copy, PartialEq, Eq)]
enum TableKind {
    Table,
    MetricView,
}

impl TableKind {
    /// Classifies a UC table resource; `None` when it has no known `table_type`.
    fn of(table: &Value) -> Option<Self> {
        match mapping::table_type(table.get("table_type").and_then(Value::as_str)).as_known() {
            Some(TableType::MetricView) => Some(Self::MetricView),
            Some(TableType::Unspecified) | None => None,
            Some(_) => Some(Self::Table),
        }
    }

    fn noun(self) -> &'static str {
        match self {
            Self::Table => "a table",
            Self::MetricView => "a metric view",
        }
    }
}

/// Compares a single table resource's kind with `expected`:
/// - `Ok(None)`: the kinds match.
/// - `Ok(Some(message))`: they differ; the message names both kinds.
/// - `Err`: the resource has no known kind (`internal`), so it is never read
///   or deleted as a table.
fn kind_mismatch(
    table: &Value,
    full_name: &str,
    expected: TableKind,
) -> Result<Option<String>, ConnectError> {
    let actual = TableKind::of(table)
        .ok_or_else(|| mapping::invalid_response("missing or unknown table_type"))?;
    Ok((actual != expected)
        .then(|| format!("{full_name} is {}, not {}", actual.noun(), expected.noun())))
}

/// Keeps the rows of one kind from a UC table list. Rows of no known kind are
/// skipped rather than failing the page, so a table type added by a newer UC
/// server does not break the list; one warning per page counts them.
fn rows_of_kind<'a>(
    rows: &'a [Value],
    kind: TableKind,
    catalog: &str,
    schema: &str,
) -> Vec<&'a Value> {
    let mut kept = Vec::new();
    let mut unclassified = Vec::new();
    for row in rows {
        match TableKind::of(row) {
            Some(actual) if actual == kind => kept.push(row),
            Some(_) => {}
            None => unclassified.push(row),
        }
    }
    if !unclassified.is_empty() {
        let examples: Vec<_> = unclassified
            .iter()
            .take(3)
            .map(|row| describe_row(row))
            .collect();
        tracing::warn!(
            catalog,
            schema,
            count = unclassified.len(),
            ?examples,
            "skipped listed tables with no known table_type"
        );
    }
    kept
}

/// Names a listed row and its raw `table_type` for a log line.
fn describe_row(row: &Value) -> String {
    let field = |key: &str| row.get(key).and_then(Value::as_str).unwrap_or("<missing>");
    format!("{} ({})", field("name"), field("table_type"))
}

fn table_path(full_name: &str) -> String {
    format!("/tables/{}", path_segment(full_name))
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
