from pyspark import SparkConf, SparkContext, StorageLevel

from .config import JobConfig
from .jobs import CooccurrenceJob, PmiJob, WordCountJob


class PmiPipeline:
    """Runs the three Spark jobs in the same order as the Hadoop driver."""

    def __init__(self, config: JobConfig, app_name: str = "PMI-Spark"):
        self.config = config
        self.sc = SparkContext(conf=SparkConf().setAppName(app_name))

    def run(self) -> str:
        """Single output: pmi_output/.
        WordCount feeds the broadcast, Cooccurrence feeds the PMI Spark Job.
        No intermediate RDDs are written to HDFS. """
        sc, cfg = self.sc, self.config
        lines = sc.textFile(cfg.input_path, minPartitions=cfg.num_partitions).persist(StorageLevel.MEMORY_AND_DISK)

        counts, total_tokens = WordCountJob(cfg).run(lines)
        word_counts_bcast = sc.broadcast(counts.collectAsMap())

        pairs = CooccurrenceJob(cfg).run(lines)
        pmi = PmiJob(cfg).run(pairs, word_counts_bcast, total_tokens)
        pmi.map(_fmt_pmi).saveAsTextFile(f"{cfg.output_base}/pmi_output")

        return f"{cfg.output_base}/pmi_output"

    def stop(self) -> None:
        self.sc.stop()


def _fmt_pmi(kv):
    (word_x, word_y), score = kv
    return f"{word_x}\t{word_y}\t{score}"
