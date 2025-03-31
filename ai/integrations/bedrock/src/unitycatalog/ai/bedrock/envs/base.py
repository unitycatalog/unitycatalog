from abc import ABC, abstractmethod


class BaseEnvVars(ABC):
    """Base class for all structured env variable loaders."""

    @abstractmethod
    def as_dict(self) -> dict:
        """Convert env values into a dict."""
        pass
