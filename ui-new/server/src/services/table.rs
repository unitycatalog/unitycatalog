use connectrpc::{ConnectError, RequestContext, Response, ServiceRequest, ServiceResult};
use serde_json::{json, Map, Value};

use super::{
    column_value, data_source_format, object_full_name, page_query, properties_value, UiRpc,
};
use crate::mapping;
use crate::proto::uc::v1::*;
use crate::upstream::path_segment;

#[protovalidate_buffa::connect_impl]
impl TableService for UiRpc {
    async fn list_tables(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListTablesRequest>,
    ) -> ServiceResult<ListTablesResponse> {
        let mut query = page_query(&request.page);
        query.extend([
            ("catalog_name", request.schema.catalog_name.to_string()),
            ("schema_name", request.schema.name.to_string()),
            ("omit_columns", "true".to_string()),
            ("omit_properties", "true".to_string()),
        ]);
        let value = self.upstream.get(&ctx, "/tables", query).await?;
        let tables = mapping::required_array(&value, "tables")?
            .iter()
            .filter(|table| table.get("table_type").and_then(Value::as_str) != Some("METRIC_VIEW"))
            .map(mapping::table_summary)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(ListTablesResponse {
            tables,
            page: mapping::page(&value).into(),
            ..Default::default()
        }))
    }

    async fn get_table(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetTableRequest>,
    ) -> ServiceResult<GetTableResponse> {
        let full_name = object_full_name(&request.table);
        let value = self
            .upstream
            .get(
                &ctx,
                &format!("/tables/{}", path_segment(&full_name)),
                Vec::new(),
            )
            .await?;
        if value.get("table_type").and_then(Value::as_str) == Some("METRIC_VIEW") {
            return Err(ConnectError::not_found(format!(
                "Table not found: {full_name}"
            )));
        }
        Ok(Response::new(GetTableResponse {
            table: mapping::table_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn create_table(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateTableRequest>,
    ) -> ServiceResult<CreateTableResponse> {
        let columns = request
            .columns
            .iter()
            .enumerate()
            .map(|(position, column)| column_value(column, position, Map::new()))
            .collect::<Result<Vec<_>, _>>()?;
        let mut body = Map::new();
        body.insert("name".to_string(), json!(request.table.name));
        body.insert(
            "catalog_name".to_string(),
            json!(request.table.catalog_name),
        );
        body.insert("schema_name".to_string(), json!(request.table.schema_name));
        body.insert("table_type".to_string(), json!("EXTERNAL"));
        body.insert(
            "data_source_format".to_string(),
            json!(data_source_format(request.data_source_format)?),
        );
        body.insert("columns".to_string(), Value::Array(columns));
        body.insert(
            "storage_location".to_string(),
            json!(request.storage_location.value),
        );
        if let Some(comment) = request.comment {
            body.insert("comment".to_string(), json!(comment));
        }
        if request.properties.is_set() {
            body.insert(
                "properties".to_string(),
                properties_value(&request.properties),
            );
        }
        let value = self
            .upstream
            .post(&ctx, "/tables", Value::Object(body))
            .await?;
        Ok(Response::new(CreateTableResponse {
            table: mapping::table_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn delete_table(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteTableRequest>,
    ) -> ServiceResult<DeleteTableResponse> {
        let full_name = object_full_name(&request.table);
        self.upstream
            .delete(
                &ctx,
                &format!("/tables/{}", path_segment(&full_name)),
                Vec::new(),
            )
            .await?;
        Ok(Response::new(DeleteTableResponse::default()))
    }
}
