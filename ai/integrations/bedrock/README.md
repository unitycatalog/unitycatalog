# unitycatalog-bedrock

This package provides integration between OSS Unity Catalog functions and AWS Bedrock. Support for Databricks Unity Catalog will be added in the next release.

---

## Installation

Install the package from the current directory:

```bash
pip install unitycatalog-bedrock
```

---

## Core User Journey (CUJ)

This section outlines the key steps to set up, configure, and use the Unity Catalog Bedrock integration.

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

---

### Step 6: Invoke the Agent

Use the `invoke_agent` method to interact with the Bedrock agent and invoke Unity Catalog functions.

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
)

print(response.response_body)
```

---

### Step 7: Handle Tool Responses

The `unitycatalog-bedrock` utilities abstract the process of handling tool responses and continuing the conversation. These utilities automatically parse the response, execute the required Unity Catalog functions, and construct the next message for the agent. This simplifies the workflow for developers, allowing them to focus on building intelligent applications without worrying about low-level details.

---

## Configuration Variables

The Bedrock integration uses the following configuration variables. These variables are essential for customizing the behavior of the integration and ensuring it works seamlessly with your AWS Bedrock setup.

| Variable                          | Description                                      | Default Value                                             |
|-----------------------------------|--------------------------------------------------|-----------------------------------------------------------|
| `aws_profile`                     | AWS profile to use for authentication. This is useful when managing multiple AWS accounts. | `default`                                                 |
| `aws_region`                      | AWS region where Bedrock services are hosted. Ensure this matches the region of your Bedrock resources. | `us-east-1`                                               |
| `bedrock_model_id`                | The ID of the Bedrock foundation model to be used. This specifies the model that the agent will invoke. | `anthropic.claude-3-5-sonnet-20240620-v1:0`               |
| `bedrock_agent_name`              | A unique name for the Bedrock agent. If not provided, a unique name will be generated automatically. | Generated unique name                                     |
| `bedrock_agent_id`                | The unique identifier of the Bedrock agent. This is required for invoking the agent. | `None`                                                    |
| `bedrock_agent_alias_id`          | The alias ID for the Bedrock agent. Aliases allow you to manage agent versions and deployments. | `None`                                                    |
| `bedrock_session_id`              | A session ID for Bedrock interactions. This helps track and manage conversations or workflows. | `default-session`                                         |
| `bedrock_rpm_limit`               | The rate limit for Bedrock API requests per minute. This ensures compliance with AWS service quotas. | `1`                                                       |
| `require_bedrock_confirmation`    | A flag to enforce user confirmation before invoking action group functions. Set to `ENABLED` for confirmation or `DISABLED` to skip it. | `DISABLED`                                                |

### Detailed Explanation of Key Variables

1. **`aws_profile`**:  
   This variable allows you to specify which AWS profile to use for authentication. If you have multiple profiles configured in your AWS CLI, you can set this to the desired profile name. For example:
   ```bash
   export aws_profile=my-aws-profile
   ```

2. **`aws_region`**:  
   The AWS region where your Bedrock services are hosted. Ensure that this matches the region of your Bedrock foundation models and agents. For example:
   ```bash
   export aws_region=us-west-2
   ```

3. **`bedrock_model_id`**:  
   This specifies the foundation model to be used by the Bedrock agent. You can find the list of available models in the [AWS Bedrock documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/what-is-bedrock.html). For example:
   ```bash
   export bedrock_model_id=anthropic.claude-3-5-sonnet-20240620-v1:0
   ```

4. **`bedrock_agent_name`**:  
   A unique name for your Bedrock agent. This helps identify the agent in your AWS account. If not set, a unique name will be generated automatically.

5. **`bedrock_rpm_limit`**:  
   This variable controls the rate of API requests to Bedrock. It is important to set this value according to the service quotas for your account. For example:
   ```bash
   export bedrock_rpm_limit=5
   ```

6. **`require_bedrock_confirmation`**:  
   This flag determines whether user confirmation is required before invoking action group functions. Set it to `ENABLED` to prompt for confirmation or `DISABLED` to skip it. For example:
   ```bash
   export require_bedrock_confirmation=ENABLED
   ```

Configuration values are automatically saved to a `config.json` file in the project directory. You can edit this file directly or use environment variables to override the defaults.

---

## Setup

### AWS Credentials

Before using this integration, ensure your AWS credentials are properly configured.

```bash
aws configure
```

### Creating Bedrock Agents and Action Groups

The setup process includes:

- Creating IAM policies and roles for the Bedrock agent  
- Creating a Bedrock agent with specific permissions  
- Configuring action groups for the agent with Unity Catalog functions  
- Creating agent aliases for deployment  

> **Important:** When creating Bedrock agents, you must wait for the agent to be in the `READY` state before adding action groups. The agent creation is asynchronous and may take several minutes.

<pre>
# Example of waiting for agent to be ready
import time
import boto3
import logging

logger = logging.getLogger(__name__)

def wait_for_agent_ready(agent_id, max_wait_time=900, check_interval=30):
    """Wait for a Bedrock agent to be in the READY state."""
    bedrock_agent_client = boto3.client('bedrock-agent')
    
    start_time = time.time()
    elapsed_time = 0
    
    logger.info(f"Waiting for agent {agent_id} to be ready...")
    
    while elapsed_time < max_wait_time:
        try:
            response = bedrock_agent_client.get_agent(agentId=agent_id)
            status = response['agent']['agentStatus']
            
            if status == 'READY':
                logger.info(f"Agent {agent_id} is now ready")
                return True
            elif status == 'FAILED':
                logger.error(f"Agent creation failed: {response['agent'].get('agentStatusReason')}")
                return False
                
            if int(elapsed_time) % 60 < check_interval:
                logger.info(f"Current agent status: {status} (waited {int(elapsed_time)}s)")
                
        except Exception as e:
            logger.warning(f"Error checking agent status: {e}")
        
        time.sleep(check_interval)
        elapsed_time = time.time() - start_time
    
    logger.error(f"Timeout waiting for agent to be ready (waited {int(elapsed_time)}s)")
    return False

if wait_for_agent_ready(agent_id):
    # Create action groups here
    # ...
</pre>

---

## Agent Function Specifications

This package supports defining function specifications in a format compatible with [Amazon Bedrock Agents](https://docs.aws.amazon.com/bedrock/latest/userguide/agents.html), based on the [`AWS::Bedrock::Agent.Function`](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-bedrock-agent-function.html) CloudFormation resource.

Each function definition includes:

- **`name`**: A unique identifier for the function.
- **`description`**: A human-readable explanation of the function's purpose.
- **`parameters`**: A schema specifying expected inputs, including:
  - Parameter name
  - Description
  - Type (`string`, `number`, etc.)
  - Required/optional status
- **`requireConfirmation`**: Optional flag to enforce user confirmation before invocation (`ENABLED` or `DISABLED`).Set to `DISABLED` by default.

### Example Function Definition

```python
require_confirmation = "DISABLED"  # Set to "ENABLED" or "DISABLED"

def get_agent_functions(require_confirmation="DISABLED"):
    """
    Get agent functions with the specified confirmation requirement.
    
    Args:
        require_confirmation: "ENABLED" or "DISABLED" for function confirmation
        
    Returns:
        List of agent function definitions
    """
    return [
        {
            "name": "get_weather_in_celsius",
            "description": "Fetch the current weather in celsius for a given location and date",
            "parameters": {
                "location_id": {
                    "description": "The unique identifier of the location to retrieve the temperature for",
                    "required": True,
                    "type": "string",
                },
                "fetch_date": {
                    "description": "The specific date for which the temperature needs to be retrieved",
                    "required": True,
                    "type": "string",
                },
            },
            "requireConfirmation": require_confirmation,
        },
        {
            "name": "get_weather_in_fahrenheit",
            "description": "Fetch the current weather in fahrenheit for a given location and date",
            "parameters": {
                "location_id": {
                    "description": "The unique identifier of the location to retrieve the temperature for",
                    "required": True,
                    "type": "string",
                },
                "fetch_date": {
                    "description": "The specific date for which the temperature needs to be retrieved",
                    "required": True,
                    "type": "string",
                },
            },
            "requireConfirmation": require_confirmation,
        },
    ]

agent_functions = get_agent_functions()
logger.info(f"agent_functions: {agent_functions}")
```

This format enables dynamic generation of function specifications for use in Bedrock action groups, allowing flexible control over user confirmation behavior.
---

## Rate Limiting

The package includes built-in rate limiting to respect AWS Bedrock quotas. The default quota for Claude 3.5 Sonnet is **1 request per minute**, but this can be customized through the `bedrock_rpm_limit` configuration variable.

Check [AWS Service Limits](https://docs.aws.amazon.com/general/latest/gr/aws_service_limits.html) for the rate limits applicable to your account for the corresponding model.

The `retry_with_exponential_backoff` function handles both rate limiting and retries for transient errors.

---

## Examples

For detailed examples, see:

- `bedrock_setup.ipynb`: Setting up Bedrock agents and action groups  
- `bedrock_examples.ipynb`: Using the Bedrock integration with Unity Catalog

---

## Support

For issues or questions, please contact the Unity Catalog team.