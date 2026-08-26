"""Entrypoint: wires sources, pipeline stages and sinks into one Flink job.

Run on the cluster:
    docker exec flink-jobmanager /opt/flink/bin/flink run -d -py /opt/workspace/main.py
"""
from pyflink.common import WatermarkStrategy

from gielda.config import get_flink_env, load_config
from gielda.pipelines.alerts import build_alerts
from gielda.pipelines.dictionary import Enrich
from gielda.pipelines.level1 import build_l1
from gielda.pipelines.level2 import build_l2
from gielda.schemas import DICT_DESC, ENRICHED_TYPE, TICK_TYPE
from gielda.sinks import attach_l1_sink, attach_l2_sink, build_alerts_sink
from gielda.sources import build_dict_source, build_ticks_source
from gielda.utils import build_watermarks, parse_tick


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

    # Partia 2: okna minutowe -> temat posredni (at-least-once, kluczowane).
    l1 = build_l1(ticks)
    attach_l1_sink(env, l1, cfg)

    # Partia 4: wzbogacenie wynikow L1 slownikiem (broadcast, zmienny w locie).
    # UWAGA: strumien wzbogacony nie ma watermarkow (wejscie broadcast ich nie
    # emituje) - Poziom 2 nie moze uzywac timerow event-time (dzien w kluczu).
    enriched = (l1.connect(dict_stream.broadcast(DICT_DESC))
                  .process(Enrich(), output_type=ENRICHED_TYPE))

    # Partia 5: narastajacy raport dzienny -> finalne ujscie MySQL (upsert).
    attach_l2_sink(env, build_l2(enriched), cfg)

    # Partia 6: alarmy z SUROWEGO strumienia (natychmiastowy + licznikowy)
    # -> temat gielda-alerty (at-least-once = natychmiastowa widocznosc).
    build_alerts(ticks, cfg).sink_to(build_alerts_sink(cfg))

    env.execute("gielda-partia-6")


if __name__ == "__main__":
    main()
