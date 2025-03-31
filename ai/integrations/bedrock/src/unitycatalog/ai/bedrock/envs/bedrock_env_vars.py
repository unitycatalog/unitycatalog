# src/unitycatalog/ai/bedrock/envs/bedrock_env_vars.py

import json
import logging
import os
from typing import Dict, Optional

from .base import BaseEnvVars

logger = logging.getLogger(__name__)

DEFAULT_MODEL_QUOTA_CODES = {
    "anthropic.claude-3-sonnet-20240229-v1:0": "L-254CACF4",
    "anthropic.claude-3-sonnet-20240229-v1:1": "L-79E773B3",
}


class BedrockEnvVars(BaseEnvVars):
    def __init__(self, env: Optional[dict] = None):
        full_env = env if env else os.environ
        self._env = {k: v for k, v in full_env.items() if k.startswith("BEDROCK_")}

    @property
    def aws_profile(self) -> Optional[str]:
        return self._env.get("BEDROCK_AWS_PROFILE", "default")

    @property
    def aws_region(self) -> str:
        return self._env.get("BEDROCK_AWS_REGION", "us-west-2")

    @property
    def bedrock_model_id(self) -> Optional[str]:
        return self._env.get("BEDROCK_MODEL_ID")

    @property
    def bedrock_model_name(self) -> Optional[str]:
        return self._env.get("BEDROCK_MODEL_NAME")

    @property
    def bedrock_agent_id(self) -> Optional[str]:
        return self._env.get("BEDROCK_AGENT_ID")

    @property
    def bedrock_agent_alias_id(self) -> Optional[str]:
        return self._env.get("BEDROCK_AGENT_ALIAS_ID")

    @property
    def bedrock_session_id(self) -> str:
        return self._env.get("BEDROCK_SESSION_ID", "default-session")

    @property
    def bedrock_rps_limit(self) -> Optional[int]:
        val = self._env.get("BEDROCK_RPS_LIMIT")
        return int(val) if val is not None else None

    @property
    def bedrock_model_quota_codes(self) -> Dict[str, str]:
        raw = self._env.get("BEDROCK_MODEL_QUOTA_CODES")
        user_map = {}

        if raw:
            try:
                user_map = json.loads(raw)
            except json.JSONDecodeError:
                try:
                    user_map = dict(pair.split("=") for pair in raw.split(","))
                except Exception as e:
                    raise ValueError(
                        f"Invalid BEDROCK_MODEL_QUOTA_CODES format: {raw}"
                    ) from e

        return {**DEFAULT_MODEL_QUOTA_CODES, **user_map}

    @property
    def bedrock_model_quota_code(self) -> Optional[str]:
        model_id = self.bedrock_model_id
        code = self.bedrock_model_quota_codes.get(model_id)

        if model_id and model_id not in self._env.get("BEDROCK_MODEL_QUOTA_CODES", ""):
            logger.warning(f"Using default quota code for model '{model_id}': {code}")

        return code

    def as_dict(self) -> dict:
        return {
            "aws_profile": self.aws_profile,
            "aws_region": self.aws_region,
            "bedrock_model_id": self.bedrock_model_id,
            "bedrock_model_name": self.bedrock_model_name,
            "bedrock_agent_id": self.bedrock_agent_id,
            "bedrock_agent_alias_id": self.bedrock_agent_alias_id,
            "bedrock_session_id": self.bedrock_session_id,
            "bedrock_rps_limit": self.bedrock_rps_limit,
            "bedrock_model_quota_code": self.bedrock_model_quota_code,
        }
