from dataclasses import dataclass
from typing import Any, Optional, Type

from pydantic import BaseModel


@dataclass
class PydanticType:
    pydantic_type: Type
    strict: bool = False


@dataclass
class PydanticField:
    pydantic_type: Type
    description: Optional[str] = None
    default: Optional[Any] = None
    strict: bool = False


@dataclass
class PydanticFunctionInputParams:
    pydantic_model: Type[BaseModel]
    strict: bool = False
