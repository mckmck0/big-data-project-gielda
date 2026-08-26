"""Entrypoint: wires sources, pipeline stages and sinks into one Flink job.

Run on the cluster:
    docker exec flink-jobmanager /opt/flink/bin/flink run -d -py /opt/workspace/main.py
"""
from pyflink.common import Types, WatermarkStrategy

from gielda.config import get_flink_env, load_config
from gielda.pipelines.dictionary import DictReader
from gielda.schemas import DICT_DESC, TICK_TYPE
from gielda.sources import build_dict_source, build_ticks_source
from gielda.utils import build_watermarks, parse_tick
from gielda.pipelines.level1 import build_l1
from gielda.sinks import attach_l1_sink



def main():
    """Assemble and submit the job."""
    cfg = load_config()
    env = get_flink_env(cfg)
    # Ship the gielda/ package to the Python UDF workers on the TaskManager.
    # Must point at the PARENT of the package, not the package itself.
    env.add_python_file("/opt/workspace")
    print("1. Config loaded, Flink environment set up successfully.")

    ticks = (env.from_source(build_ticks_source(cfg),
                             WatermarkStrategy.no_watermarks(), cfg["kafka.topic.ticks"])
             .flat_map(parse_tick, output_type=TICK_TYPE)
             .assign_timestamps_and_watermarks(build_watermarks(cfg)))

    dict_stream = env.from_source(build_dict_source(cfg),
                                  WatermarkStrategy.no_watermarks(), cfg["kafka.topic.dict"])
    print("2. Sources created, watermarks assigned.")

    # Partia 1: console output of ticks labelled from the broadcast dictionary.
    (ticks.connect(dict_stream.broadcast(DICT_DESC))
          .process(DictReader(), output_type=Types.STRING())
          .print())

    l1 = build_l1(ticks)
    attach_l1_sink(env, l1, cfg)

    env.execute("gielda-partia-2")


if __name__ == "__main__":
    main()
