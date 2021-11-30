from abc import ABC
from dataclasses import dataclass

class NotEqualEntityLexcialException(Exception):
    pass


class Anchor(ABC):
    pass


@dataclass
class Anchor:
    anchor_text: str
    context_token: str
    anchor_title: str
    anchor_first_sent: str
    anchor_parent_title: str
    anchor_parent_first_sent: str

    def __str__(self):
        return f"{self.anchor_text}/{self.context_token}/{self.anchor_title}/{self.anchor_first_sent}/{self.anchor_parent_title}/{self.anchor_parent_first_sent}"
