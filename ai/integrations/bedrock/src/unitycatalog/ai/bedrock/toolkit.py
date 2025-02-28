from typing import Any, Dict, List, Optional, Union, Iterator
from pprint import pprint
import boto3
from pydantic import BaseModel, ConfigDict, Field, model_validator
import logging

from .utils import extract_tool_calls, extract_response_details, execute_tool_calls, generate_tool_call_session_state

from unitycatalog.ai.core.client import UnitycatalogFunctionClient
from unitycatalog.ai.core.utils.client_utils import validate_or_set_default_client
from unitycatalog.ai.core.utils.function_processing_utils import (
    generate_function_input_params_schema,
    get_tool_name,
    process_function_names,
)

import time

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
        return any('returnControl' in event
                   for event in self.raw_response.get('completion', []))

    @property
    def final_response(self) -> Optional[str]:
        """Returns the final text response if available."""
        if not self.requires_tool_execution:
            for event in self.raw_response.get('completion', []):
                if 'chunk' in event:
                    return event['chunk'].get('bytes', b'').decode('utf-8')
        return None

    @property
    def is_streaming(self) -> bool:
        """Returns True if the response is a streaming response."""
        return 'chunk' in str(self.raw_response)

    def get_stream(self) -> Iterator[str]:
        """Yields chunks from a streaming response."""
        if not self.is_streaming:
            return

        for event in self.raw_response.get('completion', []):
            if 'chunk' in event:
                chunk = event['chunk'].get('bytes', b'').decode('utf-8')
                if chunk:
                    yield chunk

    def print_stream_with_wrapping(self, stream:Any, max_line_length:int=50):
        accumulated_line = ""  # Temporary storage for the current line

        for chunk in stream:
            accumulated_line += chunk  # Add chunk to the current line
            
            while len(accumulated_line) >= max_line_length:
                print(accumulated_line[:max_line_length])  # Print fixed-length part
                accumulated_line = accumulated_line[max_line_length:]  # Keep the remainder
            
        if accumulated_line:  # Print remaining part if not empty
            print(accumulated_line)


class BedrockSession:
    """Manages a session with AWS Bedrock agent runtime."""

    def __init__(self, agent_id: str, 
                 agent_alias_id: str,
                 catalog_name: str ,
                 schema_name: str ,
                 #function_name: str,
                 ):
        """Initialize a Bedrock session."""
        self.agent_id = agent_id
        self.agent_alias_id = agent_alias_id
        self.client = boto3.client('bedrock-agent-runtime')
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        #self.function_name = function_name
        
        logger.info(
            f"Initialized BedrockSession with agent_id: {self.agent_id}, "
            f"agent_alias_id: {self.agent_alias_id}, "
            f"catalog_name: {self.catalog_name}, "
            f"schema_name: {self.schema_name}"
        ) # Debugging
    
    def invoke_agent(
            self,
            input_text: str,
            enable_trace: bool = None,
            session_id: str = None,
            session_state: dict = None,
            streaming_configurations: dict = None,
            uc_client: Optional[UnitycatalogFunctionClient] = None
    ) -> BedrockToolResponse:
        """Invoke the Bedrock agent with the given input text."""
        params = {
            'agentId': self.agent_id,
            'agentAliasId': self.agent_alias_id,
            'inputText': input_text,
        }

        if enable_trace is not None:
            params['enableTrace'] = enable_trace
        if session_id is not None:
            params['sessionId'] = session_id
        if session_state is not None:
            params['sessionState'] = session_state
        if streaming_configurations is not None:
            params['streamingConfigurations'] = streaming_configurations

         # Invoke the agent
        logger.debug(f"Invoking the agent with params:{params}") #Debugging
        response = self.client.invoke_agent(**params)
        logger.debug(f"Response from invoke agent: {response}") #Debugging

        extracted_details = extract_response_details(response)

        tool_calls = []
        final_response_body = None
        if 'chunks' in extracted_details and extracted_details["chunks"]:
            final_response_body = extracted_details['chunks']
               
        elif 'tool_calls' in extracted_details and extracted_details['tool_calls']:
            tool_calls = extracted_details['tool_calls']

            logger.debug(f"Tool Call Results: {tool_calls}") #Debugging
            if tool_calls and uc_client:
                # There is a response with UC functions to call.
                logger.debug(f"Tool Calls: {tool_calls[0]['function_name']}") #Debugging
                
                function_name_to_execute = (tool_calls[0]['function_name']).split('__')[1]
                
                # Executing the UC functions in the current python environment
                tool_results = execute_tool_calls(tool_calls, uc_client,
                                                catalog_name=self.catalog_name,
                                                schema_name=self.schema_name,
                                                function_name=function_name_to_execute)
                logger.debug(f"ToolResults: {tool_results}") #Debugging
                
                if tool_results:
                    # Generate the agent session state for the next invocation with results.
                    session_state = generate_tool_call_session_state(
                        tool_results[0], tool_calls[0])
                    logger.debug(f"SessionState from tool_results: {session_state}") #Debugging
                    
                    logger.info("Sleeping for 65 seconds before invoking the agent again.")
                    time.sleep(65) #TODO: Remove this sleep and make this exponential

                    
                logger.debug(f"SessionState before invoking agent again: {session_state}")  # Debugging
                agent_stream_config = {
                                           # Bedrock will apply safety checks every second while generating and streaming the output
                                           'applyGuardrailInterval': 1000, 
                                           'streamFinalResponse': True
                                       }
                return self.invoke_agent(input_text="",
                                        session_id=session_id,
                                        enable_trace=enable_trace,
                                        session_state=session_state,
                                        streaming_configurations=agent_stream_config,
                                        uc_client=uc_client)
        
        return BedrockToolResponse(raw_response=response, tool_calls=tool_calls, response_body=final_response_body)

class BedrockTool(BaseModel):
    """Model representing a Unity Catalog function as a Bedrock tool."""
    name: str = Field(description="The name of the function.")
    description: str = Field(description="A brief description of the function's purpose.")
    parameters: Dict[str, Any] = Field(description="The parameters schema required by the function.")
    requireConfirmation: str = Field(default="ENABLED", 
                                   description="Whether confirmation is required before executing the function.")

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def to_dict(self) -> Dict[str, Union[str, Dict[str, Any]]]:
        """Convert the tool to a dictionary format for Bedrock."""
        return {
            "name": self.name,
            "description": self.description,
            "parameters": self.parameters,
            "requireConfirmation": self.requireConfirmation
        }

class UCFunctionToolkit(BaseModel):
    """A toolkit for managing Unity Catalog functions and converting them into Bedrock tools."""
    function_names: List[str] = Field(default_factory=list)
    tools_dict: Dict[str, BedrockTool] = Field(default_factory=dict)
    client: Optional[UnitycatalogFunctionClient] = Field(default=None)

    model_config = ConfigDict(arbitrary_types_allowed=True)

    def create_session(self, agent_id: str, 
                       agent_alias_id: str,
                       catalog_name: str ,
                       schema_name: str ,
                       #function_name: str,
                       ) -> BedrockSession:
        """Creates a new Bedrock session for interacting with an agent."""
        return BedrockSession(agent_id=agent_id, 
                              agent_alias_id=agent_alias_id,
                              catalog_name=catalog_name,
                              schema_name=schema_name,
                              #function_name=function_name
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
        client: Optional[UnitycatalogFunctionClient] = None,
        function_name: Optional[str] = None,
        function_info: Optional[Any] = None,
    ) -> BedrockTool:
        """Converts a Unity Catalog function to a Bedrock tool."""
        if function_name and function_info:
            raise ValueError(
                "Only one of function_name or function_info should be provided."
            )

        client = validate_or_set_default_client(client)
        try:
            if function_name:
                function_info = client.get_function(function_name)
            elif function_info:
                function_name = function_info.full_name
            else:
                raise ValueError(
                    "Either function_name or function_info should be provided.")
        except Exception as e:
            raise ValueError(f"Failed to retrieve function info: {e}")

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

    @property
    def tools(self) -> List[BedrockTool]:
        """Gets all available tools."""
        return list(self.tools_dict.values())

    def get_tool(self, name: str) -> Optional[BedrockTool]:
        """Gets a specific tool by name."""
        return self.tools_dict.get(name)
