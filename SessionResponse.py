from dataclasses import dataclass
from typing import Self, Optional
import marshmallow_dataclass
import json
from GenerateQueryResponse import GenerateQueryResponse
from functools import singledispatchmethod


@dataclass
class SessionResponse:
    sessionId: str
    generateQueryResponse: Optional[GenerateQueryResponse]
    analysis: Optional[str]

    @singledispatchmethod
    @classmethod
    def load(cls, arg) -> Self:
        raise NotImplemented(f'load() only accepts dicts and json strings. You passed a {type(arg)}')

    @load.register
    @classmethod
    def _(cls, data: str) -> Self:
        schema = marshmallow_dataclass.class_schema(cls)()
        return schema.load(json.loads(data))

    @load.register
    @classmethod
    def _(cls, data: dict) -> Self:
        schema = marshmallow_dataclass.class_schema(cls)()
        return schema.load(data)
