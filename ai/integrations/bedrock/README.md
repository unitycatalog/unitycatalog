# Unity Catalog Bedrock Integration

This repository provides tools and utilities for integrating Unity Catalog functions with [AWS Bedrock](https://aws.amazon.com/bedrock/) agents. It includes functionality for creating Bedrock agents, managing rate limits, and invoking Unity Catalog functions as Bedrock tools.

---

## Key Features

### Toolkit Enhancements
- **`BedrockToolResponse`**: Added properties to handle streaming responses (`is_streaming`, `get_stream`).
- **`BedrockSession`**: Enhanced to support recursive tool execution and rate-limited agent invocation.
- **`UCFunctionToolkit`**: Simplified session creation and validation for Unity Catalog functions.

### Rate Limiter
- **Dynamic Rate Limiting**: Added `reduce_limit` method to dynamically adjust rate limits after throttling.
- **Retry Decorator**: Enhanced retry logic with exponential backoff, jitter, and rate-limiting integration.

### Notebook Updates
- **`bedrock_example.ipynb`**:
  - Demonstrates creating Bedrock agents, IAM roles, and action groups.
  - Shows how to invoke Unity Catalog functions as Bedrock tools.

---

## Installation

Install the package from the current directory:

```bash
pip install unitycatalog-bedrock
```

---

## Configuration Variables

The Bedrock integration uses the following environment variables for configuration:

| Variable                          | Default Value       | Description                                                                 |
|-----------------------------------|---------------------|-----------------------------------------------------------------------------|
| `AWS_PROFILE`                     | `default`           | AWS profile to use for authentication.                                     |
| `AWS_REGION`                      | `us-east-1`         | AWS region where Bedrock services are hosted.                              |
| `BEDROCK_MODEL_ID`                | `None`              | The ID of the Bedrock foundation model to be used.                         |
| `BEDROCK_AGENT_NAME`              | `None`              | A unique name for the Bedrock agent.                                       |
| `BEDROCK_AGENT_ID`                | `None`              | The unique identifier of the Bedrock agent.                                |
| `BEDROCK_AGENT_ALIAS_ID`          | `None`              | Alias ID for the Bedrock agent.                                            |
| `BEDROCK_SESSION_ID`              | `default-session`   | A session ID for Bedrock interactions.                                     |
| `BEDROCK_RPM_LIMIT`               | `1`                 | Requests-per-minute limit for Bedrock agents.                              |
| `REQUIRE_BEDROCK_CONFIRMATION`    | `DISABLED`          | Whether confirmation is required before invoking action group functions.   |

### Detailed Explanation of Key Variables

1. **`AWS_PROFILE`**:  
   This variable allows you to specify which AWS profile to use for authentication. If you have multiple profiles configured in your AWS CLI, you can set this to the desired profile name. For example:
   ```bash
   export AWS_PROFILE=my-aws-profile
   ```

2. **`AWS_REGION`**:  
   The AWS region where your Bedrock services are hosted. Ensure that this matches the region of your Bedrock foundation models and agents. For example:
   ```bash
   export AWS_REGION=us-west-2
   ```

3. **`BEDROCK_MODEL_ID`**:  
   This specifies the foundation model to be used by the Bedrock agent. You can find the list of available models in the [AWS Bedrock Documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/what-is-bedrock.html). For example:
   ```bash
   export BEDROCK_MODEL_ID=anthropic.claude-3-5-sonnet-20240620-v1:0
   ```

4. **`BEDROCK_AGENT_NAME`**:  
   A unique name for your Bedrock agent. This helps identify the agent in your AWS account. If not set, a unique name will be generated automatically.

5. **`BEDROCK_RPM_LIMIT`**:  
   This variable controls the rate of API requests to Bedrock. It is important to set this value according to the service quotas for your account. For example:
   ```bash
   export BEDROCK_RPM_LIMIT=5
   ```

6. **`REQUIRE_BEDROCK_CONFIRMATION`**:  
   This flag determines whether user confirmation is required before invoking action group functions. Set it to `ENABLED` to prompt for confirmation or `DISABLED` to skip it. For example:
   ```bash
   export REQUIRE_BEDROCK_CONFIRMATION=ENABLED
   ```

---

## Core User Journey (CUJ)

### Step 1: Install the Package

Install the `unitycatalog-bedrock` package using pip:

```bash
pip install unitycatalog-bedrock
```

---

### Step 2: Configure AWS Credentials

Ensure your AWS credentials are properly configured. Use the following command to set up your credentials:

```bash
aws configure
```

Provide the following details:
- **AWS Access Key ID**
- **AWS Secret Access Key**
- **Default region** (e.g., `us-east-1`)

For more details, refer to the [AWS CLI Configuration Guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

---

### Step 3: Define Unity Catalog Functions

Create and register functions in Unity Catalog that you want to use as tools in Bedrock workflows. For example:

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

For more information on Unity Catalog functions, see the [Databricks Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html).

---

### Step 4: Create a Bedrock Agent

Create a Bedrock agent and configure it to use Unity Catalog functions. Follow these steps:

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

   For more details, refer to the [AWS IAM Documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html).

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

   For more information, see the [AWS Bedrock Documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/what-is-bedrock.html).

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

### Step 5: Create Action Groups

Define action groups for the Bedrock agent using Unity Catalog functions.

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

For more details, refer to the [AWS Bedrock Action Groups Documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/agents.html).

---

### Step 6: Invoke the Agent

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

## Examples

For a detailed example, see:

- [`bedrock_example.ipynb`](./bedrock_example.ipynb): Demonstrates setting up Bedrock agents, creating action groups, and invoking Unity Catalog functions.

---

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any bugs or feature requests.

---