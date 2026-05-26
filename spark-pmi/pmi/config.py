from dataclasses import dataclass


@dataclass(frozen=True)
class JobConfig:
    input_path: str
    output_base: str
    window_size: int = 2
    num_partitions: int = 2
