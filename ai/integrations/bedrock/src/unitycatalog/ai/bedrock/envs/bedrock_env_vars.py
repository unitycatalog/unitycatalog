import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

from unitycatalog.ai.bedrock.envs import config_manager


class BedrockEnvVars:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            cls._instance._initialized = False
            logger.info("Creating a new instance of BedrockEnvVars")
        else:
            logger.info("Reusing the existing instance of BedrockEnvVars")
        return cls._instance

    def __init__(self, load_from_file: bool = True):
        if not self._initialized:
            # Initialize default values
            self._aws_profile = "default"
            self._aws_region = "us-east-1"
            self._bedrock_model_id = None
            self._bedrock_agent_name = None
            self._bedrock_agent_id = None
            self._bedrock_agent_alias_id = None
            self._bedrock_session_id = "default-session"
            self._bedrock_rpm_limit = 1

            # Load from config file if requested
            if load_from_file:
                self.load_from_file()

            self._initialized = True
            logger.info("Initialized BedrockEnvVars with default values")

    @classmethod
    def get_instance(cls, load_from_file: bool = True):
        """Get the singleton instance of BedrockEnvVars."""
        if cls._instance is None:
            cls._instance = cls(load_from_file)
        return cls._instance

    # -------------------- File Persistence Methods --------------------
    def save_to_file(self) -> bool:
        """Save current configuration to file."""
        config = {
            "aws_profile": self._aws_profile,
            "aws_region": self._aws_region,
            "bedrock_model_id": self._bedrock_model_id,
            "bedrock_agent_name": self._bedrock_agent_name,
            "bedrock_agent_id": self._bedrock_agent_id,
            "bedrock_agent_alias_id": self._bedrock_agent_alias_id,
            "bedrock_session_id": self._bedrock_session_id,
            "bedrock_rps_limit": self._bedrock_rpm_limit,
        }
        return config_manager.save_config(config)

    def load_from_file(self) -> bool:
        """Load configuration from file."""
        config = config_manager.load_config()
        if not config:
            return False

        # Update attributes from config
        if "aws_profile" in config:
            self._aws_profile = config["aws_profile"]
        if "aws_region" in config:
            self._aws_region = config["aws_region"]
        if "bedrock_model_id" in config:
            self._bedrock_model_id = config["bedrock_model_id"]
        if "bedrock_agent_name" in config:
            self._bedrock_agent_name = config["bedrock_agent_name"]
        if "bedrock_agent_id" in config:
            self._bedrock_agent_id = config["bedrock_agent_id"]
        if "bedrock_agent_alias_id" in config:
            self._bedrock_agent_alias_id = config["bedrock_agent_alias_id"]
        if "bedrock_session_id" in config:
            self._bedrock_session_id = config["bedrock_session_id"]
        if "bedrock_rpm_limit" in config:
            self._bedrock_rpm_limit = config["bedrock_rpm_limit"]

        logger.info("Loaded configuration from file")
        return True

    # -------------------- Property Setters with Auto-Save --------------------
    @property
    def aws_profile(self) -> str:
        return self._aws_profile

    @aws_profile.setter
    def aws_profile(self, value: str):
        self._aws_profile = value
        self.save_to_file()

    @property
    def aws_region(self) -> str:
        return self._aws_region

    @aws_region.setter
    def aws_region(self, value: str):
        self._aws_region = value
        self.save_to_file()

    @property
    def bedrock_model_id(self) -> Optional[str]:
        return self._bedrock_model_id

    @bedrock_model_id.setter
    def bedrock_model_id(self, value: str):
        self._bedrock_model_id = value
        self.save_to_file()

    @property
    def bedrock_agent_name(self) -> Optional[str]:
        return self._bedrock_agent_name

    @bedrock_agent_name.setter
    def bedrock_agent_name(self, value: str):
        self._bedrock_agent_name = value
        self.save_to_file()

    @property
    def bedrock_agent_id(self) -> Optional[str]:
        return self._bedrock_agent_id

    @bedrock_agent_id.setter
    def bedrock_agent_id(self, value: str):
        self._bedrock_agent_id = value
        self.save_to_file()

    @property
    def bedrock_agent_alias_id(self) -> Optional[str]:
        return self._bedrock_agent_alias_id

    @bedrock_agent_alias_id.setter
    def bedrock_agent_alias_id(self, value: str):
        self._bedrock_agent_alias_id = value
        self.save_to_file()

    @property
    def bedrock_session_id(self) -> str:
        return self._bedrock_session_id

    @bedrock_session_id.setter
    def bedrock_session_id(self, value: str):
        self._bedrock_session_id = value
        self.save_to_file()

    @property
    def bedrock_rpm_limit(self) -> Optional[int]:
        return self._bedrock_rpm_limit

    @bedrock_rpm_limit.setter
    def bedrock_rpm_limit(self, value: str):
        self._bedrock_rpm_limit = value
        self.save_to_file()

    # -------------------- Utilities --------------------
    def as_dict(self) -> Dict[str, Any]:
        """Return all environment variables as a dictionary."""
        return {
            "aws_profile": self.aws_profile,
            "aws_region": self.aws_region,
            "bedrock_model_id": self.bedrock_model_id,
            "bedrock_agent_name": self.bedrock_agent_name,
            "bedrock_agent_id": self.bedrock_agent_id,
            "bedrock_agent_alias_id": self.bedrock_agent_alias_id,
            "bedrock_session_id": self.bedrock_session_id,
            "bedrock_rpm_limit": self.bedrock_rpm_limit,
        }

    def reset(self):
        """Reset all values to defaults."""
        self._aws_profile = "default"
        self._aws_region = "us-east-1"
        self._bedrock_model_id = None
        self._bedrock_agent_name = None
        self._bedrock_agent_id = None
        self._bedrock_agent_alias_id = None
        self._bedrock_session_id = "default-session"
        self._bedrock_rpm_limit = 1
        logger.info("Reset BedrockEnvVars to default values")
        self.save_to_file()  # Save the reset values
