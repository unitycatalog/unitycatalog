# Unity Catalog Bedrock Integration

This project integrates Unity Catalog functions with AWS Bedrock. It provides tools to create and manage Bedrock agents, action groups, and functions.

## Installation

To install the package, run:
```bash
pip install unitycatalog-bedrock
```

## Setup

### Setting Up Unity Catalog Client

To set up the Unity Catalog client, use the following code:

```python
from unitycatalog.ai.core.client import UnitycatalogFunctionClient
from unitycatalog.client import ApiClient, Configuration

def setup_uc_client():
    """Set up Unity Catalog client"""
    config = Configuration()
    config.host = "http://0.0.0.0:8080/api/2.1/unity-catalog"
    api_client = ApiClient(configuration=config)
    return UnitycatalogFunctionClient(api_client=api_client)

client = setup_uc_client()
```

### Creating Bedrock Agents and Action Groups

To create Bedrock agents and action groups, use the following code:

```python
import boto3, json, time

# Initialize clients
iam_client = boto3.client('iam')
sts_client = boto3.client('sts')
session = boto3.session.Session()

region = session.region_name
account_id = sts_client.get_caller_identity()["Account"]

# Agent foundation model name
agent_foundation_model = "anthropic.claude-3-5-sonnet-20240620-v1:0"

# Create bedrock agent IAM policy
agent_bedrock_allow_policy_name = "ucai-iam-policy-name"
bedrock_agent_bedrock_allow_policy_statement = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AmazonBedrockAgentBedrockFoundationModelPolicy",
            "Effect": "Allow",
            "Action": "bedrock:InvokeModel",
            "Resource": [
                f"arn:aws:bedrock:{region}::foundation-model/{agent_foundation_model}"
            ]
        },
        {
            "Sid": "AmazonBedrockAgentStreamBedrockFoundationModelPolicy",
            "Effect": "Allow",
            "Action": "bedrock:InvokeModelWithResponseStream",
            "Resource": [
                f"arn:aws:bedrock:{region}::foundation-model/{agent_foundation_model}"
            ]
        }
    ]
}

bedrock_policy_json = json.dumps(bedrock_agent_bedrock_allow_policy_statement)
agent_bedrock_policy = iam_client.create_policy(
    PolicyName=agent_bedrock_allow_policy_name,
    PolicyDocument=bedrock_policy_json
)

# Create IAM Role for the agent and attach IAM policies
agent_role_name = "ucai-iam-role-name"
assume_role_policy_document = {
    "Version": "2012-10-17",
    "Statement": [{
          "Effect": "Allow",
          "Principal": {
            "Service": "bedrock.amazonaws.com"
          },
          "Action": "sts:AssumeRole"
    }]
}

assume_role_policy_document_json = json.dumps(assume_role_policy_document)
agent_role = iam_client.create_role(
    RoleName=agent_role_name,
    AssumeRolePolicyDocument=assume_role_policy_document_json
)

# Pause to make sure role is created
time.sleep(10)

iam_client.attach_role_policy(
    RoleName=agent_role_name,
    PolicyArn=agent_bedrock_policy['Policy']['Arn']
)

# Create bedrock agent
agent_name = "ucai-bra-name"
agent_description = "ucai-bra-description"
agent_instruction = "You are a weather agent to fetch the current weather in celsius and fahrenheit for a given location"

bedrock_agent_client = boto3.client('bedrock-agent')
create_agent_response = bedrock_agent_client.create_agent(
    agentName=agent_name,
    agentResourceRoleArn=agent_role['Role']['Arn'],
    description=agent_description,
    idleSessionTTLInSeconds=1800,
    foundationModel=agent_foundation_model,
    instruction=agent_instruction,
)

agent_id = create_agent_response['agent']['agentId']

# Prepare the function specifications
agent_functions = [
    {
        'name': 'get_weather_in_celsius',
        'description': 'Fetch the current weather in celsius for a given location and date',
        'parameters': {
            "location_id": {
                "description": "The unique identifier of the location to retrieve the temperature for",
                "required": True,
                "type": "string"
            },
            "fetch_date": {
                "description": "The specific date for which the temperature needs to be retrieved",
                "required": True,
                "type": "string"
            }
        },
        'requireConfirmation':'ENABLED'
    },
    {
        'name': 'get_weather_in_fahrenheit',
        'description': 'Fetch the current weather in fahrenheit for a given location and date',
        'parameters': {
            "location_id": {
                "description": "The unique identifier of the location to retrieve the temperature for",
                "required": True,
                "type": "string"
            },
            "fetch_date": {
                "description": "The specific date for which the temperature needs to be retrieved",
                "required": True,
                "type": "string"
            }
        },
        'requireConfirmation':'ENABLED'
    }
]

# Prepare agent group using function schema
agent_action_group_name = "ucai-bda-action-group-name"
agent_action_group_description = "Actions to fetch the weather of a given location for a given date"

agent_action_group_response = bedrock_agent_client.create_agent_action_group(
    agentId=agent_id,
    agentVersion='DRAFT',
    actionGroupExecutor={
        'customControl': 'RETURN_CONTROL'
    },
    actionGroupName=agent_action_group_name,
    functionSchema={
        'functions': agent_functions
    },
    description=agent_action_group_description
)

response = bedrock_agent_client.prepare_agent(
    agentId=agent_id
)

# Create bedrock agent alias
agent_alias_name = "ucai-bra-alias-name"
agent_alias_description = "Alias for the weather agent"

create_alias_response = bedrock_agent_client.create_agent_alias(
    agentId=agent_id,
    agentAliasName=agent_alias_name,
    description=agent_alias_description
)

agent_alias_id = create_alias_response['agentAlias']['agentAliasId']
```

## Usage

### Using the Toolkit

To use the toolkit with Unity Catalog functions and Bedrock agents, follow these steps:

```python
from unitycatalog.ai.bedrock import UCFunctionToolkit

# Create toolkit with function names
function_names = ["AICatalog.AISchema.get_weather_in_celsius", "AICatalog.AISchema.get_weather_in_fahrenheit"]
uc_f_toolkit = UCFunctionToolkit(function_names=function_names, client=client)

# Create a session with your Bedrock agent
session = uc_f_toolkit.create_session(agent_id="your_agent_id", 
                                      agent_alias_id="your_alias_id",
                                      catalog_name="AICatalog",
                                      schema_name="AISchema")

# Invoke the agent
response = session.invoke_agent("What is the weather in Centigrade and Fahrenheit for location 12345 and date of 2025-11-19?",
                                enable_trace=True,
                                session_id="your_session_id",
                                uc_client=uc_f_toolkit.client)

print(response.response_body)
```

### Listing Functions

To list functions in Unity Catalog, use the following code:

```python
uc_functions = client.list_functions(
    catalog="AICatalog",
    schema="AISchema",
    max_results=10
)

import pprint
pprint.pp(uc_functions)
```

This README provides a comprehensive guide to setting up and using the Unity Catalog Bedrock integration. For more detailed examples, refer to the provided notebooks: `bedrock_examples.ipynb`, `bedrock_setup.ipynb`, and `uc_setup.ipynb`.
