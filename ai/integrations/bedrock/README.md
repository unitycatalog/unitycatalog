# unitycatalog-bedrock

This package provides integration between OSS Unity Catalog functions and AWS Bedrock. Support for Databricks Unity Catalog will be added in the next release. 

---

## Installation

Install the package from the current directory:

```bash
pip install unitycatalog-bedrock
```

---

## Configuration Variables

The Bedrock integration uses the following configuration variables:

| Variable                          | Description                                      | Default Value                                             |
|-----------------------------------|--------------------------------------------------|-----------------------------------------------------------|
| `aws_profile`                     | AWS profile to use for authentication            | `default`                                                 |
| `aws_region`                      | AWS region for Bedrock services                  | `us-east-1`                                               |
| `bedrock_model_id`                | ID of the Bedrock foundation model               | `anthropic.claude-3-5-sonnet-20240620-v1:0`               |
| `bedrock_agent_name`              | Name of the Bedrock agent                        | Generated unique name                                     |
| `bedrock_agent_id`                | ID of the Bedrock agent                          | `None`                                                    |
| `bedrock_agent_alias_id`          | ID of the Bedrock agent alias                    | `None`                                                    |
| `bedrock_session_id`              | Session ID for Bedrock interactions              | `default-session`                                         |
| `bedrock_rpm_limit`               | Rate limit for Bedrock API requests per minute   | `1`                                                        |
| `require_bedrock_confirmation`    | Get user confirmation before invoking action group function               |`DISABLED`                                                            |

Configuration values are automatically saved to a `config.json` file in the project directory.

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

## Agent Function Specifications---

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

require_confirmation = "DISABLED"
agent_functions = get_agent_functions(require_confirmation)
logger.info(f"agent_functions: {agent_functions}")
```

This format enables dynamic generation of function specifications for use in Bedrock action groups, allowing flexible control over user confirmation behavior.---

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