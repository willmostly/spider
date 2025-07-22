from dataclasses import dataclass, asdict
import json

@dataclass
class CreateSessionRequest:
    dataProductId: str
    rawQuestion: str

    def to_json(self):
        return json.dumps(asdict(self))

    def asdict(self):
        return asdict(self)
