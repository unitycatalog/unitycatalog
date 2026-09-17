use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult};
use serde_json::{json, Map, Value};

use super::{page_query, volume_type};
use crate::mapping;
use crate::proto::uc::v1::*;
use crate::upstream::{path_segment, Upstream};
use crate::AppState;

pub(crate) struct VolumeRpc {
    upstream: Upstream,
}

impl VolumeRpc {
    pub(crate) fn new(state: AppState) -> Self {
        Self {
            upstream: Upstream::new(state),
        }
    }
}

#[protovalidate_buffa::connect_impl]
impl VolumeService for VolumeRpc {
    async fn list_volumes(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListVolumesRequest>,
    ) -> ServiceResult<ListVolumesResponse> {
        let request = request.to_owned_message();
        let mut query = page_query(&request.page);
        query.extend([
            ("catalog_name", request.schema.catalog_name.clone()),
            ("schema_name", request.schema.name.clone()),
        ]);
        let value = self.upstream.get(&ctx, "/volumes", query).await?;
        let volumes = mapping::required_array(&value, "volumes")?
            .iter()
            .map(mapping::volume_summary)
            .collect();
        Ok(Response::new(ListVolumesResponse {
            volumes,
            page: mapping::page(&value).into(),
            ..Default::default()
        }))
    }

    async fn get_volume(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetVolumeRequest>,
    ) -> ServiceResult<GetVolumeResponse> {
        let request = request.to_owned_message();
        let value = self
            .upstream
            .get(
                &ctx,
                &format!("/volumes/{}", path_segment(&full_name(&request.volume))),
                Vec::new(),
            )
            .await?;
        Ok(Response::new(GetVolumeResponse {
            volume: mapping::volume_info(&value).into(),
            ..Default::default()
        }))
    }

    async fn create_volume(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateVolumeRequest>,
    ) -> ServiceResult<CreateVolumeResponse> {
        let request = request.to_owned_message();
        let mut body = Map::new();
        body.insert("name".to_string(), json!(request.volume.name));
        body.insert(
            "catalog_name".to_string(),
            json!(request.volume.catalog_name),
        );
        body.insert("schema_name".to_string(), json!(request.volume.schema_name));
        body.insert(
            "volume_type".to_string(),
            json!(volume_type(request.volume_type)?),
        );
        if let Some(comment) = request.comment {
            body.insert("comment".to_string(), json!(comment));
        }
        if request.storage_location.is_set() {
            body.insert(
                "storage_location".to_string(),
                json!(request.storage_location.value),
            );
        }
        let value = self
            .upstream
            .post(&ctx, "/volumes", Value::Object(body))
            .await?;
        Ok(Response::new(CreateVolumeResponse {
            volume: mapping::volume_info(&value).into(),
            ..Default::default()
        }))
    }

    async fn update_volume(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, UpdateVolumeRequest>,
    ) -> ServiceResult<UpdateVolumeResponse> {
        let request = request.to_owned_message();
        let name = full_name(&request.volume);
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
                &format!("/volumes/{}", path_segment(&name)),
                Value::Object(body),
            )
            .await?;
        Ok(Response::new(UpdateVolumeResponse {
            volume: mapping::volume_info(&value).into(),
            ..Default::default()
        }))
    }

    async fn delete_volume(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteVolumeRequest>,
    ) -> ServiceResult<DeleteVolumeResponse> {
        let request = request.to_owned_message();
        self.upstream
            .delete(
                &ctx,
                &format!("/volumes/{}", path_segment(&full_name(&request.volume))),
                Vec::new(),
            )
            .await?;
        Ok(Response::new(DeleteVolumeResponse::default()))
    }
}

fn full_name(reference: &SchemaObjectRef) -> String {
    format!(
        "{}.{}.{}",
        reference.catalog_name, reference.schema_name, reference.name
    )
}
