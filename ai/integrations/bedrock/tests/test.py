"""
import boto3
import functools
import random
import time
from threading import Semaphore
from botocore.exceptions import ClientError

# --- Static quota code map (extend as needed) ---
MODEL_QUOTA_CODES = {
    "anthropic.claude-3-sonnet-20240229-v1:0": "L-254CACF4",
    # Add more models if needed
}

# --- Fetch quota for the model ---
def get_model_quota(model_id, region='us-west-2', profile_name=None):
    quota_code = MODEL_QUOTA_CODES.get(model_id)
    if not quota_code:
        raise ValueError(f"Quota code not defined for model {model_id}")

    session = boto3.Session(profile_name=profile_name) if profile_name else boto3.Session()
    client = session.client("service-quotas", region_name=region)
    response = client.get_service_quota(ServiceCode='bedrock', QuotaCode=quota_code)
    return int(response["Quota"]["Value"])

# --- Retry decorator with exponential backoff and jitter ---
def retry_with_exponential_backoff(max_retries=5, base_delay=1, max_delay=20, exceptions=(ClientError,)):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retry = 0
            delay = base_delay
            while retry < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    error_code = e.response["Error"]["Code"]
                    if error_code != "ThrottlingException":
                        raise  # only retry on throttling
                    retry += 1
                    if retry >= max_retries:
                        raise
                    sleep_time = min(max_delay, random.uniform(base_delay, delay * 3))
                    print(f"⏱️ Retry #{retry} after {sleep_time:.2f}s due to throttling...")
                    time.sleep(sleep_time)
                    delay = sleep_time
        return wrapper
    return decorator

# --- Throttler class using real quota ---
class BedrockThrottler:
    def __init__(self, model_id, region='us-west-2', profile_name=None):
        self.rps_limit = get_model_quota(model_id, region=region, profile_name=profile_name)
        self.semaphore = Semaphore(self.rps_limit)

    def run(self, func, *args, **kwargs):
        with self.semaphore:
            return func(*args, **kwargs)

# --- Bedrock Agent call wrapped with retry ---
@retry_with_exponential_backoff(max_retries=5, base_delay=1)
def invoke_bedrock_agent(agent_id, agent_alias_id, session_id, input_text, profile_name=None, region="us-west-2"):
    session = boto3.Session(profile_name=profile_name) if profile_name else boto3.Session()
    client = session.client("bedrock-agent-runtime", region_name=region)

    try:
        response = client.invoke_agent(
            agentId=agent_id,
            agentAliasId=agent_alias_id,
            sessionId=session_id,
            inputText=input_text  # ✅ Correct param
        )
        print("✅ Agent response received")
        return response

    except ClientError as e:
        print(f"⚠️ Bedrock error: {e.response['Error']['Code']}")
        raise

# --- Test driver ---
def test_bedrock_agent_with_throttling():
    # 🔁 Replace these with your actual values
    agent_id = "HXPGL1POGD"
    agent_alias_id = "RB4BBCHKP2"
    model_id = "anthropic.claude-3-sonnet-20240229-v1:0"
    session_id = "test-session-001"
    input_text = "Hello Bedrock agent!"
    profile_name = None

    throttler = BedrockThrottler(model_id, profile_name=profile_name)
    print(f"🚦 RPS limit from quota: {throttler.rps_limit} req/min\n")

    for i in range(10):
        print(f"\n🚀 Request #{i+1}")
        try:
            response = throttler.run(
                invoke_bedrock_agent,
                agent_id,
                agent_alias_id,
                session_id,
                input_text,
                profile_name
            )
            print(f"📝 Response status: {response['ResponseMetadata']['HTTPStatusCode']}")
            print(f"📝 Response status: {response}")
        except Exception as e:
            print(f"💥 Request failed after retries: {e}")

# --- Entry Point ---
if __name__ == "__main__":
    test_bedrock_agent_with_throttling()
"""
