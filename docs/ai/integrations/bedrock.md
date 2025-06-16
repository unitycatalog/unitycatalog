# Using Unity Catalog AI with [AWS Bedrock](https://aws.amazon.com/bedrock/)

Integrate Unity Catalog AI with [AWS Bedrock](https://aws.amazon.com/bedrock/) to utilize functions defined in Unity Catalog as tools in Bedrock agent calls. This guide covers installation, setup, environment variables, public APIs, and examples to help you get started.

---

## Installation

Install the Unity Catalog AI Bedrock integration from the current directory:

```bash
pip install unitycatalog-bedrock
```

---

## Prerequisites

- **Python version**: Python 3.9 or higher is required.
- **AWS Credentials**: Ensure your AWS credentials are properly configured using [`aws configure`](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html).

### Unity Catalog

Ensure that you have a functional [Unity Catalog](https://www.databricks.com/product/unity-catalog) server set up and that you are able to access the catalog and schema where defined functions are stored.

---

## Tutorial

### Step 1: Client Setup

Create an instance of the Bedrock client:

```python
import boto3
from unitycatalog.ai.core.client import UnitycatalogFunctionClient

# Create the Unity Catalog client
client = UnitycatalogFunctionClient()

# Create the Bedrock client
bedrock_client = boto3.client("bedrock-runtime")
```

---

### Step 2: Define Unity Catalog Functions

Define and register functions in Unity Catalog that you want to use as tools in Bedrock workflows:

```python
CATALOG = "AICatalog"
SCHEMA = "AISchema"

def get_weather_in_celsius(location_id: str, fetch_date: str) -> str:
    """
    Fetches weather data (in Celsius) for a given location and date.
    """
    return str(23)

def get_weather_in_fahrenheit(location_id: str, fetch_date: str) -> str:
    """
    Fetches weather data (in Fahrenheit) for a given location and date.
    """
    return str(72)

client.create_python_function(
    func=get_weather_in_celsius, catalog=CATALOG, schema=SCHEMA, replace=True
)

client.create_python_function(
    func=get_weather_in_fahrenheit, catalog=CATALOG, schema=SCHEMA, replace=True
)
```

---

### Step 3: Create a Bedrock Agent

Create a Bedrock agent and configure it to use Unity Catalog functions:

1. **Create IAM Policies and Roles**:  
   Define IAM policies and roles for the Bedrock agent to invoke foundation models.

   ```python
   iam_client = boto3.client("iam")
   sts_client = boto3.client("sts")
   account_id = sts_client.get_caller_identity()["Account"]

   policy_statement = {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Effect": "Allow",
               "Action": "bedrock:InvokeModel",
               "Resource": f"arn:aws:bedrock:{bedrock_env.aws_region}::foundation-model/{bedrock_env.bedrock_model_id}",
           }
       ],
   }

   policy = iam_client.create_policy(
       PolicyName="bedrock-agent-policy", PolicyDocument=json.dumps(policy_statement)
   )
   ```

2. **Create the Agent**:  
   Use the AWS SDK to create a Bedrock agent.

   ```python
   bedrock_agent_client = boto3.client("bedrock-agent")
   create_agent_response = bedrock_agent_client.create_agent(
       agentName="weather-agent",
       agentResourceRoleArn="arn:aws:iam::account_id:role/bedrock-agent-role",
       foundationModel="anthropic.claude-3-5-sonnet-20240620-v1:0",
       instruction="You are a weather agent to fetch the current weather.",
   )
   ```

3. **Wait for Agent Readiness**:  
   Ensure the agent is in the `READY` state before proceeding.

   ```python
   def wait_for_agent_ready(agent_id):
       while True:
           response = bedrock_agent_client.get_agent(agentId=agent_id)
           if response["agent"]["agentStatus"] == "READY":
               break
           time.sleep(30)
   ```

---

### Step 4: Create Action Groups

Define action groups for the Bedrock agent using Unity Catalog functions:

```python
agent_functions = [
    {
        "name": "get_weather_in_celsius",
        "description": "Fetch the current weather in Celsius for a given location and date.",
        "parameters": {
            "location_id": {"description": "Location ID", "required": True, "type": "string"},
            "fetch_date": {"description": "Date", "required": True, "type": "string"},
        },
    },
    {
        "name": "get_weather_in_fahrenheit",
        "description": "Fetch the current weather in Fahrenheit for a given location and date.",
        "parameters": {
            "location_id": {"description": "Location ID", "required": True, "type": "string"},
            "fetch_date": {"description": "Date", "required": True, "type": "string"},
        },
    },
]

action_group_response = bedrock_agent_client.create_agent_action_group(
    agentId="your_agent_id",
    actionGroupName="weather-actions",
    functionSchema={"functions": agent_functions},
)
```

---

### Step 5: Invoke the Agent

Use the `invoke_agent` method to interact with the Bedrock agent and invoke Unity Catalog functions. The method now supports specifying `model_id` and `rpm_limit`.

```python
import uuid

# Generate a unique session ID
session_id = str(uuid.uuid1())

# Invoke the agent
response = uc_f_toolkit.create_session(
    agent_id=bedrock_env.bedrock_agent_id,
    agent_alias_id=bedrock_env.bedrock_agent_alias_id,
    catalog_name=CATALOG,
    schema_name=SCHEMA,
).invoke_agent(
    input_text="What is the weather in Celsius and Fahrenheit for location 12345 on 2025-02-26?",
    enable_trace=True,
    session_id=session_id,
    uc_client=uc_f_toolkit.client,
    model_id=bedrock_env.bedrock_model_id,  # Specify the model ID
    rpm_limit=bedrock_env.bedrock_rpm_limit,  # Specify the RPM limit
)

print(response.response_body)
```

---

### Step 6: Handle Tool Responses

The `unitycatalog-bedrock` utilities abstract the process of handling tool responses and continuing the conversation. These utilities automatically parse the response, execute the required Unity Catalog functions, and construct the next message for the agent. This simplifies the workflow for developers, allowing them to focus on building intelligent applications without worrying about low-level details.

---

## Additional Notes

- **Confirmation**: Use the `requireConfirmation` flag in function definitions to enforce user confirmation before invocation.
- **Multiple Tool Calls**: Bedrock may request multiple tools in a single response. Ensure your application can handle multiple calls to UC tools if needed.

For more control, use the `extract_tool_call_data` utility:

```python
from unitycatalog.ai.bedrock.utils import extract_tool_call_data

parsed_messages = extract_tool_call_data(response)

results = []
for message in parsed_messages:
    tool_result = message.execute(client)
    results.append(message.to_tool_result_message(tool_result))
```

---

## Rate Limiting

The package includes built-in rate limiting to respect [AWS Bedrock quotas](https://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html). Customize the rate limit using the `bedrock_rpm_limit` configuration variable.

---
