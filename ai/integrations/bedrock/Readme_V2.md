# unitycatalog-bedrock

This package provides integration between Unity Catalog functions and AWS Bedrock.

---

## Installation

Install the package from the current directory:

```bash
pip install unitycatalog-bedrock
```

---

## Configuration Variables

The Bedrock integration uses the following configuration variables:

| Variable               | Description                                      | Default Value                                             |
|------------------------|--------------------------------------------------|-----------------------------------------------------------|
| `aws_profile`          | AWS profile to use for authentication            | `default`                                                 |
| `aws_region`           | AWS region for Bedrock services                  | `us-east-1`                                               |
| `bedrock_model_id`     | ID of the Bedrock foundation model               | `anthropic.claude-3-5-sonnet-20240620-v1:0`              |
| `bedrock_agent_name`   | Name of the Bedrock agent                        | Generated unique name                                     |
| `bedrock_agent_id`     | ID of the Bedrock agent                          | `None`                                                    |
| `bedrock_agent_alias_id` | ID of the Bedrock agent alias                  | `None`                                                    |
| `bedrock_session_id`   | Session ID for Bedrock interactions              | `default-session`                                         |
| `bedrock_rpm_limit`    | Rate limit for Bedrock API requests per minute  | `1`                                                       |

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

---
<pre> ```python # 
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
                
            # Log status periodically
            if int(elapsed_time) % 60 < check_interval:
                logger.info(f"Current agent status: {status} (waited {int(elapsed_time)}s)")
                
        except Exception as e:
            logger.warning(f"Error checking agent status: {e}")
        
        time.sleep(check_interval)
        elapsed_time = time.time() - start_time
    
    logger.error(f"Timeout waiting for agent to be ready (waited {int(elapsed_time)}s)")
    return False

# Only create action groups after agent is ready
if wait_for_agent_ready(agent_id):
    # Create action groups here
    # ...

 ``` </pre>

## Rate Limiting

The package includes built-in rate limiting to respect AWS Bedrock quotas. The default quota for Claude 3.5 Sonnet is **1 request per minute**, but this can be customized through the `bedrock_rpm_limit` configuration variable.

The `retry_with_exponential_backoff` function handles both rate limiting and retries for transient errors.

---

## Examples

For detailed examples, see:

- `bedrock_setup.ipynb`: Setting up Bedrock agents and action groups  
- `bedrock_examples.ipynb`: Using the Bedrock integration with Unity Catalog

---

## Support

For issues or questions, please contact the Unity Catalog team.
