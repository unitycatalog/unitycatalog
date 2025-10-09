from unitycatalog.ai.bedrock.envs.base import BaseEnvVars


class BedrockEnvVars(BaseEnvVars):
    """Environment variable manager for Bedrock configuration."""

    @property
    def aws_profile(self) -> str:
        return self.get_env("AWS_PROFILE", default="default")

    @aws_profile.setter
    def aws_profile(self, value: str):
        self.set_env("AWS_PROFILE", value)

    @property
    def aws_region(self) -> str:
        return self.get_env("AWS_REGION", default="us-east-1")

    @aws_region.setter
    def aws_region(self, value: str):
        self.set_env("AWS_REGION", value)

    @property
    def bedrock_model_id(self) -> str:
        return self.get_env("BEDROCK_MODEL_ID", default=None)

    @bedrock_model_id.setter
    def bedrock_model_id(self, value: str):
        self.set_env("BEDROCK_MODEL_ID", value)

    @property
    def bedrock_agent_name(self) -> str:
        return self.get_env("BEDROCK_AGENT_NAME", default=None)

    @bedrock_agent_name.setter
    def bedrock_agent_name(self, value: str):
        self.set_env("BEDROCK_AGENT_NAME", value)

    @property
    def bedrock_agent_id(self) -> str:
        return self.get_env("BEDROCK_AGENT_ID", default=None)

    @bedrock_agent_id.setter
    def bedrock_agent_id(self, value: str):
        self.set_env("BEDROCK_AGENT_ID", value)

    @property
    def bedrock_agent_alias_id(self) -> str:
        return self.get_env("BEDROCK_AGENT_ALIAS_ID", default=None)

    @bedrock_agent_alias_id.setter
    def bedrock_agent_alias_id(self, value: str):
        self.set_env("BEDROCK_AGENT_ALIAS_ID", value)

    @property
    def bedrock_session_id(self) -> str:
        return self.get_env("BEDROCK_SESSION_ID", default="default-session")

    @bedrock_session_id.setter
    def bedrock_session_id(self, value: str):
        self.set_env("BEDROCK_SESSION_ID", value)

    @property
    def bedrock_rpm_limit(self) -> int:
        return int(self.get_env("BEDROCK_RPM_LIMIT", default=1))

    @bedrock_rpm_limit.setter
    def bedrock_rpm_limit(self, value: int):
        self.set_env("BEDROCK_RPM_LIMIT", str(value))

    @property
    def require_bedrock_confirmation(self) -> str:
        return self.get_env("REQUIRE_BEDROCK_CONFIRMATION", default="DISABLED")

    @require_bedrock_confirmation.setter
    def require_bedrock_confirmation(self, value: str):
        self.set_env("REQUIRE_BEDROCK_CONFIRMATION", value)
