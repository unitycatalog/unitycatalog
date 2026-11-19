use connectrpc::{RequestContext, Response, ServiceRequest, ServiceResult};
use serde_json::{json, Value};

use super::{object_full_name, page_query, scalar_type, UiRpc};
use crate::mapping;
use crate::proto::uc::v1::*;
use crate::upstream::path_segment;

#[protovalidate_buffa::connect_impl]
impl FunctionService for UiRpc {
    async fn list_functions(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, ListFunctionsRequest>,
    ) -> ServiceResult<ListFunctionsResponse> {
        let mut query = page_query(&request.page);
        query.extend([
            ("catalog_name", request.schema.catalog_name.to_string()),
            ("schema_name", request.schema.name.to_string()),
        ]);
        let value = self.upstream.get(&ctx, "/functions", query).await?;
        let functions = mapping::required_array(&value, "functions")?
            .iter()
            .map(mapping::function_summary)
            .collect::<Result<Vec<_>, _>>()?;
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
        let value = self
            .upstream
            .get(
                &ctx,
                &format!(
                    "/functions/{}",
                    path_segment(&object_full_name(&request.function))
                ),
                Vec::new(),
            )
            .await?;
        Ok(Response::new(GetFunctionResponse {
            function: mapping::function_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn create_function(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, CreateFunctionRequest>,
    ) -> ServiceResult<CreateFunctionResponse> {
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
            function: mapping::function_info(&value)?.into(),
            ..Default::default()
        }))
    }

    async fn delete_function(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, DeleteFunctionRequest>,
    ) -> ServiceResult<DeleteFunctionResponse> {
        self.upstream
            .delete(
                &ctx,
                &format!(
                    "/functions/{}",
                    path_segment(&object_full_name(&request.function))
                ),
                Vec::new(),
            )
            .await?;
        Ok(Response::new(DeleteFunctionResponse::default()))
    }
}
