use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult};
use serde_json::{json, Map, Value};

use super::{object_full_name, page_query, UiRpc};
use crate::mapping;
use crate::proto::uc::v1::__buffa::view::ModelVersionRefView;
use crate::proto::uc::v1::*;
use crate::upstream::{path_segment, Upstream};

#[protovalidate_buffa::connect_impl]
impl ModelService for UiRpc {
    async fn list_registered_models(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListRegisteredModelsRequest>,
    ) -> ServiceResult<ListRegisteredModelsResponse> {
        let mut query = page_query(&request.page);
        if request.schema.is_set() {
            query.extend([
                ("catalog_name", request.schema.catalog_name.to_string()),
                ("schema_name", request.schema.name.to_string()),
            ]);
        }
        let value = self.upstream.get(&ctx, "/models", query).await?;
        let models = mapping::required_array(&value, "registered_models")?
            .iter()
            .map(mapping::model_summary)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(ListRegisteredModelsResponse {
            models,
            page: mapping::page(&value).into(),
            ..Default::default()
        }))
    }

    async fn get_registered_model(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetRegisteredModelRequest>,
    ) -> ServiceResult<GetRegisteredModelResponse> {
        let value = self
            .upstream
            .get(
                &ctx,
                &format!(
                    "/models/{}",
                    path_segment(&object_full_name(&request.model))
                ),
                Vec::new(),
            )
            .await?;
        Ok(Response::new(GetRegisteredModelResponse {
            model: mapping::model_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn create_registered_model(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateRegisteredModelRequest>,
    ) -> ServiceResult<CreateRegisteredModelResponse> {
        let mut body = json!({
            "name": request.model.name,
            "catalog_name": request.model.catalog_name,
            "schema_name": request.model.schema_name,
        });
        if let Some(comment) = request.comment {
            body["comment"] = json!(comment);
        }
        let value = self.upstream.post(&ctx, "/models", body).await?;
        Ok(Response::new(CreateRegisteredModelResponse {
            model: mapping::model_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn update_registered_model(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateRegisteredModelRequest>,
    ) -> ServiceResult<UpdateRegisteredModelResponse> {
        let name = object_full_name(&request.model);
        let mut body = Map::new();
        if let Some(comment) = request.comment {
            body.insert("comment".to_string(), json!(comment));
        }
        if let Some(new_name) = request.new_name {
            body.insert("new_name".to_string(), json!(new_name));
        }
        let value = self
            .upstream
            .patch(
                &ctx,
                &format!("/models/{}", path_segment(&name)),
                Value::Object(body),
            )
            .await?;
        Ok(Response::new(UpdateRegisteredModelResponse {
            model: mapping::model_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn delete_registered_model(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteRegisteredModelRequest>,
    ) -> ServiceResult<DeleteRegisteredModelResponse> {
        self.upstream
            .delete(
                &ctx,
                &format!(
                    "/models/{}",
                    path_segment(&object_full_name(&request.model))
                ),
                vec![("force", request.force.to_string())],
            )
            .await?;
        Ok(Response::new(DeleteRegisteredModelResponse::default()))
    }

    async fn list_model_versions(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListModelVersionsRequest>,
    ) -> ServiceResult<ListModelVersionsResponse> {
        let value = self
            .upstream
            .get(
                &ctx,
                &format!(
                    "/models/{}/versions",
                    path_segment(&object_full_name(&request.model))
                ),
                page_query(&request.page),
            )
            .await?;
        let versions = mapping::required_array(&value, "model_versions")?
            .iter()
            .map(mapping::model_version_info)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(ListModelVersionsResponse {
            versions,
            page: mapping::page(&value).into(),
            ..Default::default()
        }))
    }

    async fn get_model_version(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetModelVersionRequest>,
    ) -> ServiceResult<GetModelVersionResponse> {
        let value = get_version(&self.upstream, &ctx, &request.model_version).await?;
        Ok(Response::new(GetModelVersionResponse {
            model_version: mapping::model_version_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn create_model_version(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateModelVersionRequest>,
    ) -> ServiceResult<CreateModelVersionResponse> {
        let mut body = json!({
            "model_name": request.model.name,
            "catalog_name": request.model.catalog_name,
            "schema_name": request.model.schema_name,
            "source": request.source,
        });
        if let Some(run_id) = request.run_id {
            body["run_id"] = json!(run_id);
        }
        if let Some(comment) = request.comment {
            body["comment"] = json!(comment);
        }
        let value = self.upstream.post(&ctx, "/models/versions", body).await?;
        Ok(Response::new(CreateModelVersionResponse {
            model_version: mapping::model_version_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn update_model_version(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateModelVersionRequest>,
    ) -> ServiceResult<UpdateModelVersionResponse> {
        let value = self
            .upstream
            .patch(
                &ctx,
                &version_path(&request.model_version),
                json!({ "comment": request.comment }),
            )
            .await?;
        Ok(Response::new(UpdateModelVersionResponse {
            model_version: mapping::model_version_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn finalize_model_version(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, FinalizeModelVersionRequest>,
    ) -> ServiceResult<FinalizeModelVersionResponse> {
        let model = object_full_name(&request.model_version.model);
        let value = self
            .upstream
            .patch(
                &ctx,
                &format!("{}/finalize", version_path(&request.model_version)),
                json!({
                    "full_name": model,
                    "version": request.model_version.version,
                }),
            )
            .await?;
        Ok(Response::new(FinalizeModelVersionResponse {
            model_version: mapping::model_version_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn delete_model_version(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteModelVersionRequest>,
    ) -> ServiceResult<DeleteModelVersionResponse> {
        self.upstream
            .delete(&ctx, &version_path(&request.model_version), Vec::new())
            .await?;
        Ok(Response::new(DeleteModelVersionResponse::default()))
    }
}

async fn get_version(
    upstream: &Upstream,
    ctx: &RequestContext,
    reference: &ModelVersionRefView<'_>,
) -> Result<Value, connectrpc::ConnectError> {
    upstream
        .get(ctx, &version_path(reference), Vec::new())
        .await
}

fn version_path(reference: &ModelVersionRefView<'_>) -> String {
    format!(
        "/models/{}/versions/{}",
        path_segment(&object_full_name(&reference.model)),
        reference.version
    )
}
