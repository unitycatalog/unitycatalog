import logging
from typing import Any, Dict, Iterator, List, Optional, Union

import boto3
from pydantic import BaseModel, ConfigDict, Field, model_validator

from unitycatalog.ai.bedrock.envs.bedrock_env_vars import BedrockEnvVars
from unitycatalog.ai.bedrock.utils import (
    execute_tool_calls,
    extract_response_details,
    generate_tool_call_session_state,
    retry_with_exponential_backoff,
)
from unitycatalog.ai.core.client import UnitycatalogFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)

# Setup AWS credentials if available
boto3.setup_default_session()

logger = logging.getLogger(__name__)


class BedrockToolResponse(BaseModel):
    """Class to handle Bedrock agent responses and tool calls."""

    raw_response: Dict[str, Any]
    tool_calls: List[Dict[str, Any]] = Field(default_factory=list)
    tool_results: List[Dict[str, Any]] = Field(default_factory=list)
    response_body: Optional[Any] = None

    @property
    def requires_tool_execution(self) -> bool:
        """Returns True if the response requires tool execution."""
        return any(
            event.get("returnControl") is True for event in self.raw_response.get("completion", [])
        )

    @property
    def final_response(self) -> Optional[str]:
        """Returns the final text response if available."""
        if not self.requires_tool_execution:
            for event in self.raw_response.get("completion", []):
                if "chunk" in event:
                    return event["chunk"].get("bytes", b"").decode("utf-8")
        return None

    @property
    def is_streaming(self) -> bool:
        """Returns True if the response is a streaming response."""
        return any(
            "chunk" in event and "bytes" in event["chunk"]
            for event in self.raw_response.get("completion", [])
        )

    def get_stream(self) -> Iterator[str]:
        """Yields chunks from a streaming response."""
        if not self.is_streaming:
            return

        for event in self.raw_response.get("completion", []):
            if chunk := event.get("chunk", {}).get("bytes", b"").decode("utf-8"):
                yield chunk

    def print_stream_with_wrapping(self, stream: Any, max_line_length: int = 50):
        buffer = []
        current_length = 0

        for chunk in stream:
            buffer.append(chunk)
            current_length += len(chunk)

            if current_length >= max_line_length:
                accumulated_line = "".join(buffer)
                full_lines = len(accumulated_line) // max_line_length
                for i in range(full_lines):
                    logger.info(accumulated_line[i * max_line_length : (i + 1) * max_line_length])
                leftover = accumulated_line[full_lines * max_line_length :]
                buffer = [leftover] if leftover else []
                current_length = len(leftover)

        if buffer:
            logger.info("".join(buffer))


class BedrockSession:
    """Manages a session with AWS Bedrock agent runtime."""

    def __init__(
        self,
        agent_id: str,
        agent_alias_id: str,
        catalog_name: str,
        schema_name: str,
    ):
        self.agent_id = agent_id
        self.agent_alias_id = agent_alias_id
        self.client = boto3.client("bedrock-agent-runtime")
        self.catalog_name = catalog_name
        self.schema_name = schema_name

    def _invoke_agent_with_backoff(
        self,
        input_text,
        session_state,
        session_id,
        enable_trace,
        streaming_configurations,
        uc_client,
    ):
        """Invokes the agent with exponential backoff logic."""

        def invoke():
            return self.invoke_agent(
                input_text=input_text,
                session_id=session_id,
                enable_trace=enable_trace,
                session_state=session_state,
                streaming_configurations=streaming_configurations,
                uc_client=uc_client,
            )

        return retry_with_exponential_backoff(invoke)

    def invoke_agent(
        self,
        input_text: str,
        enable_trace: bool = None,
        session_id: str = None,
        session_state: dict = None,
        streaming_configurations: dict = None,
        uc_client: UnitycatalogFunctionClient = None,
    ) -> BedrockToolResponse:
        """Invoke the Bedrock agent with the given input text."""

        params = {
            "agentId": self.agent_id,
            "agentAliasId": self.agent_alias_id,
            "inputText": input_text,
            **{
                k: v
                for k, v in {
                    "enableTrace": enable_trace,
                    "sessionId": session_id,
                    "sessionState": session_state,
                    "streamingConfigurations": streaming_configurations,
                }.items()
                if v is not None
            },
        }

        response = self.client.invoke_agent(**params)

        extracted_details = extract_response_details(response)

        tool_calls = []
        final_response_body = None
        if "chunks" in extracted_details and extracted_details["chunks"]:
            final_response_body = extracted_details["chunks"]

        elif "tool_calls" in extracted_details and extracted_details["tool_calls"]:
            tool_calls = extracted_details["tool_calls"]

            logger.debug(f"Tool Call Results: {tool_calls}")
            if tool_calls:
                function_name_to_execute = (tool_calls[0]["function_name"]).split("__")[1]

                tool_results = execute_tool_calls(
                    tool_calls,
                    uc_client,
                    catalog_name=self.catalog_name,
                    schema_name=self.schema_name,
                    function_name=function_name_to_execute,
                )
                logger.debug(f"ToolResults: {tool_results}")

                if tool_results:
                    session_state = generate_tool_call_session_state(tool_results[0], tool_calls[0])

                agent_stream_config = {
                    # Bedrock will apply safety checks every second while generating and streaming the output
                    "applyGuardrailInterval": 1000,
                    "streamFinalResponse": True,
                }
                return self._invoke_agent_with_backoff(
                    input_text="",
                    session_id=session_id,
                    enable_trace=enable_trace,
                    session_state=session_state,
                    streaming_configurations=agent_stream_config,
                    uc_client=uc_client,
                )

        return BedrockToolResponse(
            raw_response=response,
            tool_calls=tool_calls,
            response_body=final_response_body,
        )


class BedrockTool(BaseModel):
    """Model representing a Unity Catalog function as a Bedrock tool."""

    name: str = Field(description="The name of the function.")
    description: str = Field(description="A brief description of the function's purpose.")
    parameters: Dict[str, Any] = Field(
        description="The parameters schema required by the function."
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def to_dict(self) -> Dict[str, Union[str, Dict[str, Any]]]:
        """Convert the tool to a dictionary format for Bedrock."""
        bedrock_env = BedrockEnvVars.get_instance(load_from_file=True)
        requireConfirmation: str = Field(
            default=bedrock_env.require_bedrock_confirmation,
            description="Whether confirmation is required before executing the function.",
        )
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters,
            "requireConfirmation": requireConfirmation,
        }


class UCFunctionToolkit(BaseModel):
    """A toolkit for managing Unity Catalog functions and converting them into Bedrock tools."""

    function_names: List[str] = Field(default_factory=list)
    tools_dict: Dict[str, BedrockTool] = Field(default_factory=dict)
    client: Optional[UnitycatalogFunctionClient] = Field(default=None)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def create_session(
        self,
        agent_id: str,
        agent_alias_id: str,
        catalog_name: str,
        schema_name: str,
    ) -> BedrockSession:
        """Creates a new Bedrock session for interacting with an agent."""
        return BedrockSession(
            agent_id=agent_id,
            agent_alias_id=agent_alias_id,
            catalog_name=catalog_name,
            schema_name=schema_name,
        )

    @model_validator(mode="after")
    def validate_toolkit(self) -> "UCFunctionToolkit":
        """Validates and processes the toolkit configuration."""
        self.client = validate_or_set_default_client(self.client)
        self.tools_dict = process_function_names(
            function_names=self.function_names,
            tools_dict=self.tools_dict,
            client=self.client,
            uc_function_to_tool_func=self.uc_function_to_bedrock_tool,
        )
        return self

    @staticmethod
    def uc_function_to_bedrock_tool(
        *,
        client: UnitycatalogFunctionClient,
        function_name: str,
    ) -> BedrockTool:
        """Converts a Unity Catalog function to a Bedrock tool."""
        if not function_name:
            raise ValueError("A valid function_name must be provided.")

        client = validate_or_set_default_client(client)
        try:
            function_info = client.get_function(function_name)
        except Exception as e:
            raise ValueError(f"Failed to retrieve function info for {function_name}: {e}") from e

        fn_schema = generate_function_input_params_schema(function_info)
        parameters = {
            "type": "object",
            "properties": fn_schema.pydantic_model.model_json_schema().get("properties", {}),
            "required": fn_schema.pydantic_model.model_json_schema().get("required", []),
        }

        return BedrockTool(
            name=get_tool_name(function_name),
            description=function_info.comment or "",
            parameters=parameters,
        )
