from dataclasses import dataclass

@dataclass
class Posting:
    indexrecipeid: int
    score: float = 0.0