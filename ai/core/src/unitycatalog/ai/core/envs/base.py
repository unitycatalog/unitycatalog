import json
import os
import typing


class _EnvironmentVariable:
    def __init__(
        self,
        name: str,
        type_: type,
        default_value: any,
        description: str,
        element_type: type = str,
    ):
        self.name = name
        # Support parameterized generics such as ``list[str]``: normalize to the
        # origin type (``list``) so the ``is list`` checks below work, and infer
        # the element type from the type arguments when present. Without this,
        # ``list[str] is list`` is False and a list-valued variable falls through
        # to ``list[str](raw_value)``, splitting the string into single characters.
        origin = typing.get_origin(type_)
        if origin is not None:
            args = typing.get_args(type_)
            if args:
                element_type = args[0]
            type_ = origin
        self.type = type_
        self.description = description
        self.element_type = element_type
        if type_ is str:
            self.default_value = str(default_value)
        elif type_ is list:
            if isinstance(default_value, list):
                self.default_value = [self.element_type(x) for x in default_value]
            else:
                self.default_value = default_value
        else:
            self.default_value = default_value

    def _get_raw(self) -> str:
        return os.getenv(self.name)

    def get(self) -> any:
        raw_val = self._get_raw()
        if raw_val is not None:
            try:
                if self.type is list:
                    try:
                        data = json.loads(raw_val)
                    except Exception:
                        data = [x.strip() for x in raw_val.split(",")]
                    return [self.element_type(item) for item in data]
                else:
                    return self.type(raw_val)
            except Exception as e:
                raise ValueError(
                    f"Failed to convert {raw_val!r} to {self.type} for {self.name}"
                ) from e
        return self.default_value

    def set(self, value: any) -> None:
        os.environ[self.name] = json.dumps(value) if self.type is list else str(value)

    def remove(self) -> None:
        os.environ.pop(self.name, None)

    def __repr__(self) -> str:
        return (
            f"Environment variable for {self.name}. Default value: {self.default_value}. "
            f"Usage: {self.description}"
            if self.description
            else ""
        )
