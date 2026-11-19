use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult};
use serde_json::{json, Map, Value};

use super::{page_query, properties_value, UiRpc};
use crate::mapping;
use crate::proto::uc::v1::*;
use crate::upstream::path_segment;

#[protovalidate_buffa::connect_impl]
impl SchemaService for UiRpc {
    async fn list_schemas(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListSchemasRequest>,
    ) -> ServiceResult<ListSchemasResponse> {
        let mut query = page_query(&request.page);
        query.push(("catalog_name", request.catalog.name.to_string()));
        let value = self.upstream.get(&ctx, "/schemas", query).await?;
        let schemas = mapping::required_array(&value, "schemas")?
            .iter()
            .map(mapping::schema_summary)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(ListSchemasResponse {
            schemas,
            page: mapping::page(&value).into(),
            ..Default::default()
        }))
    }

    async fn get_schema(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetSchemaRequest>,
    ) -> ServiceResult<GetSchemaResponse> {
        let full_name = format!("{}.{}", request.schema.catalog_name, request.schema.name);
        let value = self
            .upstream
            .get(
                &ctx,
                &format!("/schemas/{}", path_segment(&full_name)),
                Vec::new(),
            )
            .await?;
        Ok(Response::new(GetSchemaResponse {
            schema: mapping::schema_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn create_schema(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateSchemaRequest>,
    ) -> ServiceResult<CreateSchemaResponse> {
        let mut body = Map::new();
        body.insert("name".to_string(), json!(request.schema.name));
        body.insert(
            "catalog_name".to_string(),
            json!(request.schema.catalog_name),
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
        if let Some(storage_root) = request.storage_root {
            body.insert("storage_root".to_string(), json!(storage_root));
        }
        let value = self
            .upstream
            .post(&ctx, "/schemas", Value::Object(body))
            .await?;
        Ok(Response::new(CreateSchemaResponse {
            schema: mapping::schema_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn update_schema(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateSchemaRequest>,
    ) -> ServiceResult<UpdateSchemaResponse> {
        let full_name = format!("{}.{}", request.schema.catalog_name, request.schema.name);
        let mut body = Map::new();
        if let Some(comment) = request.comment {
            body.insert("comment".to_string(), json!(comment));
        }
        if request.properties.is_set() {
            body.insert(
                "properties".to_string(),
                properties_value(&request.properties),
            );
        }
        if let Some(new_name) = request.new_name {
            body.insert("new_name".to_string(), json!(new_name));
        }
        let value = self
            .upstream
            .patch(
                &ctx,
                &format!("/schemas/{}", path_segment(&full_name)),
                Value::Object(body),
            )
            .await?;
        Ok(Response::new(UpdateSchemaResponse {
            schema: mapping::schema_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn delete_schema(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteSchemaRequest>,
    ) -> ServiceResult<DeleteSchemaResponse> {
        let full_name = format!("{}.{}", request.schema.catalog_name, request.schema.name);
        self.upstream
            .delete(
                &ctx,
                &format!("/schemas/{}", path_segment(&full_name)),
                vec![("force", request.force.to_string())],
            )
            .await?;
        Ok(Response::new(DeleteSchemaResponse::default()))
    }
}
