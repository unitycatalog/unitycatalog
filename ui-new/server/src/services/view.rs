use std::collections::{HashMap, HashSet};

use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use serde_json::{json, Map, Value};

use super::{column_value, continue_page, dependencies_value, page_query, properties_value};
use crate::mapping;
use crate::proto::uc::v1::*;
use crate::upstream::{path_segment, Upstream};
use crate::AppState;

pub(crate) struct ViewRpc {
    upstream: Upstream,
}

impl ViewRpc {
    pub(crate) fn new(state: AppState) -> Self {
        Self {
            upstream: Upstream::new(state),
        }
    }
}

#[protovalidate_buffa::connect_impl]
impl ViewService for ViewRpc {
    async fn list_views(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListViewsRequest>,
    ) -> ServiceResult<ListViewsResponse> {
        let request = request.to_owned_message();
        let mut query = page_query(&request.page);
        query.extend([
            ("catalog_name", request.schema.catalog_name.clone()),
            ("schema_name", request.schema.name.clone()),
            ("omit_columns", "true".to_string()),
            ("omit_properties", "true".to_string()),
        ]);
        let (views, page) = loop {
            let value = self.upstream.get(&ctx, "/tables", query.clone()).await?;
            let views: Vec<_> = mapping::required_array(&value, "tables")?
                .iter()
                .filter(|table| {
                    table.get("table_type").and_then(Value::as_str) == Some("METRIC_VIEW")
                })
                .map(mapping::metric_view_summary)
                .collect();
            let page = mapping::page(&value);
            if !views.is_empty() || page.next_page_token.is_empty() {
                break (views, page);
            }
            continue_page(&mut query, &page.next_page_token)?;
        };
        Ok(Response::new(ListViewsResponse {
            views,
            page: page.into(),
            ..Default::default()
        }))
    }

    async fn get_view(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetViewRequest>,
    ) -> ServiceResult<GetViewResponse> {
        let request = request.to_owned_message();
        let name = full_name(&request.view);
        let value = self
            .upstream
            .get(
                &ctx,
                &format!("/tables/{}", path_segment(&name)),
                Vec::new(),
            )
            .await?;
        if value.get("table_type").and_then(Value::as_str) != Some("METRIC_VIEW") {
            return Err(ConnectError::not_found(format!(
                "Metric view not found: {name}"
            )));
        }
        Ok(Response::new(GetViewResponse {
            view: mapping::metric_view_info(&value).into(),
            ..Default::default()
        }))
    }

    async fn create_view(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateViewRequest>,
    ) -> ServiceResult<CreateViewResponse> {
        let request = request.to_owned_message();
        let fields = metric_fields(&request.view_definition)?;
        let mut names = HashSet::new();
        let mut columns = Vec::with_capacity(request.columns.len());
        for (position, column) in request.columns.iter().enumerate() {
            if !names.insert(column.name.as_str()) {
                return Err(ConnectError::invalid_argument(format!(
                    "Metric view column names must be unique: {}",
                    column.name
                )));
            }
            let (kind, expression) = fields.get(&column.name).ok_or_else(|| {
                ConnectError::invalid_argument(format!(
                    "Column {} is not declared in the metric view definition",
                    column.name
                ))
            })?;
            let mut metadata = Map::new();
            metadata.insert("metric_view.type".to_string(), json!(kind));
            metadata.insert("metric_view.expr".to_string(), json!(expression));
            columns.push(column_value(column, position, metadata)?);
        }
        if names.len() != fields.len() {
            let mut missing: Vec<_> = fields
                .keys()
                .filter(|name| !names.contains(name.as_str()))
                .cloned()
                .collect();
            missing.sort();
            return Err(ConnectError::invalid_argument(format!(
                "Metric view columns are missing YAML fields: {}",
                missing.join(", ")
            )));
        }
        let mut body = json!({
            "name": request.view.name,
            "catalog_name": request.view.catalog_name,
            "schema_name": request.view.schema_name,
            "table_type": "METRIC_VIEW",
            "columns": columns,
            "view_definition": request.view_definition,
            "view_dependencies": dependencies_value(&request.dependencies),
        });
        if let Some(comment) = request.comment {
            body["comment"] = json!(comment);
        }
        if request.properties.is_set() {
            body["properties"] = properties_value(&request.properties);
        }
        let value = self.upstream.post(&ctx, "/tables", body).await?;
        Ok(Response::new(CreateViewResponse {
            view: mapping::metric_view_info(&value).into(),
            ..Default::default()
        }))
    }

    async fn delete_view(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteViewRequest>,
    ) -> ServiceResult<DeleteViewResponse> {
        let request = request.to_owned_message();
        let name = full_name(&request.view);
        let value = self
            .upstream
            .get(
                &ctx,
                &format!("/tables/{}", path_segment(&name)),
                Vec::new(),
            )
            .await?;
        if value.get("table_type").and_then(Value::as_str) != Some("METRIC_VIEW") {
            return Err(ConnectError::not_found(format!(
                "Metric view not found: {name}"
            )));
        }
        self.upstream
            .delete(
                &ctx,
                &format!("/tables/{}", path_segment(&name)),
                Vec::new(),
            )
            .await?;
        Ok(Response::new(DeleteViewResponse::default()))
    }
}

fn metric_fields(
    definition: &str,
) -> Result<HashMap<String, (&'static str, String)>, ConnectError> {
    let yaml: serde_yaml::Value = serde_yaml::from_str(definition)
        .map_err(|_| ConnectError::invalid_argument("Invalid metric view YAML definition"))?;
    let mut fields = HashMap::new();
    collect_metric_fields(&yaml, "dimensions", "dimension", &mut fields)?;
    collect_metric_fields(&yaml, "measures", "measure", &mut fields)?;
    Ok(fields)
}

fn collect_metric_fields(
    yaml: &serde_yaml::Value,
    section: &str,
    kind: &'static str,
    fields: &mut HashMap<String, (&'static str, String)>,
) -> Result<(), ConnectError> {
    let Some(entries) = yaml.get(section) else {
        return Ok(());
    };
    let entries = entries
        .as_sequence()
        .ok_or_else(|| ConnectError::invalid_argument(format!("{section} must be a YAML list")))?;
    for entry in entries {
        let name = entry
            .get("name")
            .and_then(serde_yaml::Value::as_str)
            .ok_or_else(|| {
                ConnectError::invalid_argument(format!("{section} entries require a name"))
            })?;
        let expression = entry
            .get("expr")
            .and_then(serde_yaml::Value::as_str)
            .ok_or_else(|| {
                ConnectError::invalid_argument(format!("{section} entries require an expr"))
            })?;
        if fields
            .insert(name.to_string(), (kind, expression.to_string()))
            .is_some()
        {
            return Err(ConnectError::invalid_argument(format!(
                "Metric view field names must be unique: {name}"
            )));
        }
    }
    Ok(())
}

fn full_name(reference: &SchemaObjectRef) -> String {
    format!(
        "{}.{}.{}",
        reference.catalog_name, reference.schema_name, reference.name
    )
}
