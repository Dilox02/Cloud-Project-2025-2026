import sys

from pmi.config import JobConfig
from pmi.pipeline import PmiPipeline


def main() -> None:
    if not 3 <= len(sys.argv) <= 5:
        raise SystemExit(
            "Usage: spark-submit main.py <input> <output_base> [<window> <partitions>]"
        )
    config = JobConfig(
        input_path=sys.argv[1],
        output_base=sys.argv[2],
        window_size=int(sys.argv[3]) if len(sys.argv) >= 4 else 2,
        num_partitions=int(sys.argv[4]) if len(sys.argv) >= 5 else 2,
    )
    pipeline = PmiPipeline(config)
    try:
        print(f"Pipeline completed. Output: {pipeline.run()}")
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main()
