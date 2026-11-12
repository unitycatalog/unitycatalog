use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult};
use serde_json::{json, Map, Value};

use super::{page_query, properties_value};
use crate::mapping;
use crate::proto::uc::v1::*;
use crate::upstream::{path_segment, Upstream};
use crate::AppState;

pub(crate) struct CatalogRpc {
    upstream: Upstream,
}

impl CatalogRpc {
    pub(crate) fn new(state: AppState) -> Self {
        Self {
            upstream: Upstream::new(state),
        }
    }
}

#[protovalidate_buffa::connect_impl]
impl CatalogService for CatalogRpc {
    async fn list_catalogs(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListCatalogsRequest>,
    ) -> ServiceResult<ListCatalogsResponse> {
        let request = request.to_owned_message();
        let value = self
            .upstream
            .get(&ctx, "/catalogs", page_query(&request.page))
            .await?;
        let catalogs = mapping::required_array(&value, "catalogs")?
            .iter()
            .map(mapping::catalog_summary)
            .collect();
        Ok(Response::new(ListCatalogsResponse {
            catalogs,
            page: mapping::page(&value).into(),
            ..Default::default()
        }))
    }

    async fn get_catalog(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetCatalogRequest>,
    ) -> ServiceResult<GetCatalogResponse> {
        let request = request.to_owned_message();
        let value = self
            .upstream
            .get(
                &ctx,
                &format!("/catalogs/{}", path_segment(&request.catalog.name)),
                Vec::new(),
            )
            .await?;
        Ok(Response::new(GetCatalogResponse {
            catalog: mapping::catalog_info(&value).into(),
            ..Default::default()
        }))
    }

    async fn create_catalog(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateCatalogRequest>,
    ) -> ServiceResult<CreateCatalogResponse> {
        let request = request.to_owned_message();
        let mut body = Map::new();
        body.insert("name".to_string(), json!(request.name));
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
            .post(&ctx, "/catalogs", Value::Object(body))
            .await?;
        Ok(Response::new(CreateCatalogResponse {
            catalog: mapping::catalog_info(&value).into(),
            ..Default::default()
        }))
    }

    async fn update_catalog(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateCatalogRequest>,
    ) -> ServiceResult<UpdateCatalogResponse> {
        let request = request.to_owned_message();
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
                &format!("/catalogs/{}", path_segment(&request.catalog.name)),
                Value::Object(body),
            )
            .await?;
        Ok(Response::new(UpdateCatalogResponse {
            catalog: mapping::catalog_info(&value).into(),
            ..Default::default()
        }))
    }

    async fn delete_catalog(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteCatalogRequest>,
    ) -> ServiceResult<DeleteCatalogResponse> {
        let request = request.to_owned_message();
        self.upstream
            .delete(
                &ctx,
                &format!("/catalogs/{}", path_segment(&request.catalog.name)),
                vec![("force", request.force.to_string())],
            )
            .await?;
        Ok(Response::new(DeleteCatalogResponse::default()))
    }
}
