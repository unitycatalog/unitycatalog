import json
import os


class _EnvironmentVariable:
    def __init__(self, name: str, type_: type, default_value: str, description: str):
        self.name = name
        self.type = type_
        self.default_value = str(default_value) if type_ is str else default_value
        self.description = description

    def _get_raw(self) -> str:
        return os.getenv(self.name)

    def get(self) -> any:
        raw_val = self._get_raw()
        if raw_val is not None:
            try:
                if self.type is list:
                    try:
                        return json.loads(raw_val)
                    except Exception:
                        return [x.strip() for x in raw_val.split(",")]
                else:
                    return self.type(raw_val)
            except Exception as e:
                raise ValueError(
                    f"Failed to convert {raw_val!r} to {self.type} for {self.name}"
                ) from e
        return self.default_value

    def set(self, value: str) -> None:
        os.environ[self.name] = str(value)

    def remove(self) -> None:
        os.environ.pop(self.name, None)

    def __repr__(self) -> str:
        return f"Environment variable for {self.name}. Default value: {self.default_value}. " + (
            f"Usage: {self.description}" if self.description else ""
        )
