from abc import ABC
from dataclasses import dataclass

class NotEqualEntityLexcialException(Exception):
    pass


@dataclass
class Anchor:
    id: int
    anchor_title: str
    anchor_first_sent: str
    anchor_parent_title: str
    anchor_parent_first_sent: str

    def __str__(self):
        return f"{self.id}/{self.anchor_title}/{self.anchor_first_sent}/{self.anchor_parent_title}/{self.anchor_parent_first_sent}"

@dataclass
class AnchorInstance:
    anchor_title_id: int
    anchor_text: str
    context_token: str

    def __str__(self):
        return f"{self.anchor_title_id}/{self.anchor_text}/{self.context_token}"
