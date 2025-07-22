from dataclasses import dataclass
from typing import Dict, List, Self
import marshmallow_dataclass
import json


@dataclass
class GenerateQueryResponse:
    reasoning: str
    query: str
    columnDefinitions: Dict[str, Dict[str, str]]
    currencyColumns: List[str]

    @classmethod
    def load(cls, data: str) -> Self:
        schema = marshmallow_dataclass.class_schema(cls)()
        return schema.load(json.loads(data))
