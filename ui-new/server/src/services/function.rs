use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult};
use serde_json::{json, Value};

use super::{page_query, scalar_type};
use crate::mapping;
use crate::proto::uc::v1::*;
use crate::upstream::{path_segment, Upstream};
use crate::AppState;

pub(crate) struct FunctionRpc {
    upstream: Upstream,
}

impl FunctionRpc {
    pub(crate) fn new(state: AppState) -> Self {
        Self {
            upstream: Upstream::new(state),
        }
    }
}

#[protovalidate_buffa::connect_impl]
impl FunctionService for FunctionRpc {
    async fn list_functions(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListFunctionsRequest>,
    ) -> ServiceResult<ListFunctionsResponse> {
        let request = request.to_owned_message();
        let mut query = page_query(&request.page);
        query.extend([
            ("catalog_name", request.schema.catalog_name.clone()),
            ("schema_name", request.schema.name.clone()),
        ]);
        let value = self.upstream.get(&ctx, "/functions", query).await?;
        let functions = mapping::required_array(&value, "functions")?
            .iter()
            .map(mapping::function_summary)
            .collect();
        Ok(Response::new(ListFunctionsResponse {
            functions,
            page: mapping::page(&value).into(),
            ..Default::default()
        }))
    }

    async fn get_function(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, GetFunctionRequest>,
    ) -> ServiceResult<GetFunctionResponse> {
        let request = request.to_owned_message();
        let value = self
            .upstream
            .get(
                &ctx,
                &format!("/functions/{}", path_segment(&full_name(&request.function))),
                Vec::new(),
            )
            .await?;
        Ok(Response::new(GetFunctionResponse {
            function: mapping::function_info(&value).into(),
            ..Default::default()
        }))
    }

    async fn create_function(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateFunctionRequest>,
    ) -> ServiceResult<CreateFunctionResponse> {
        let request = request.to_owned_message();
        let parameters = request
            .parameters
            .iter()
            .enumerate()
            .map(|(position, parameter)| {
                let data_type = scalar_type(parameter.type_name)?;
                Ok(json!({
                    "name": parameter.name,
                    "type_text": data_type.text,
                    "type_json": json!({ "type": data_type.spark_json }).to_string(),
                    "type_name": data_type.name,
                    "position": position,
                    "parameter_mode": "IN",
                    "parameter_type": "PARAM",
                    "comment": parameter.comment,
                }))
            })
            .collect::<Result<Vec<Value>, connectrpc::ConnectError>>()?;
        let return_type = scalar_type(request.return_type)?;
        let mut body = json!({
            "name": request.function.name,
            "catalog_name": request.function.catalog_name,
            "schema_name": request.function.schema_name,
            "input_params": { "parameters": parameters },
            "data_type": return_type.name,
            "full_data_type": return_type.text,
            "routine_body": "SQL",
            "routine_definition": request.routine_definition,
            "parameter_style": "S",
            "is_deterministic": true,
            "sql_data_access": "NO_SQL",
            "is_null_call": true,
            "security_type": "DEFINER",
            "specific_name": request.function.name,
        });
        if let Some(comment) = request.comment {
            body["comment"] = json!(comment);
        }
        let value = self
            .upstream
            .post(&ctx, "/functions", json!({ "function_info": body }))
            .await?;
        Ok(Response::new(CreateFunctionResponse {
            function: mapping::function_info(&value).into(),
            ..Default::default()
        }))
    }

    async fn delete_function(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteFunctionRequest>,
    ) -> ServiceResult<DeleteFunctionResponse> {
        let request = request.to_owned_message();
        self.upstream
            .delete(
                &ctx,
                &format!("/functions/{}", path_segment(&full_name(&request.function))),
                Vec::new(),
            )
            .await?;
        Ok(Response::new(DeleteFunctionResponse::default()))
    }
}

fn full_name(reference: &SchemaObjectRef) -> String {
    format!(
        "{}.{}.{}",
        reference.catalog_name, reference.schema_name, reference.name
    )
}
