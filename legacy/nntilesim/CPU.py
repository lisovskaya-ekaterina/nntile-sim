from dataclasses import dataclass, field

@dataclass
class CPU: 
    memory : list = field(default_factory=list)