"""Sklada caly pipeline gieldy w jeden job Flinka.

Uruchomienie:
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
    cfg = load_config()
    env = get_flink_env(cfg)
    # pakiet gielda/ musi trafic na workery Pythona; podajemy RODZICA pakietu
    env.add_python_file("/opt/workspace")
    print("[1/6] konfiguracja wczytana, srodowisko Flink skonfigurowane")

    ticks = (env.from_source(build_ticks_source(cfg),
                             WatermarkStrategy.no_watermarks(), cfg["kafka.topic.ticks"])
             .flat_map(parse_tick, output_type=TICK_TYPE)
             .assign_timestamps_and_watermarks(build_watermarks(cfg)))
    dict_stream = env.from_source(build_dict_source(cfg),
                                  WatermarkStrategy.no_watermarks(), cfg["kafka.topic.dict"])
    print("[2/6] zrodla Kafka podpiete (ticki + slownik), watermarki ustawione")

    # Partia 2: okna minutowe -> temat posredni (kluczowane, at-least-once)
    l1 = build_l1(ticks)
    attach_l1_sink(env, l1, cfg)
    print("[3/6] Poziom 1: okna 1-minutowe + sink na temat posredni")

    # Partia 4: wzbogacenie slownikiem; za broadcastem nie plynie watermark,
    # wiec dalej nie wolno uzywac timerow event-time (stad dzien w kluczu L2)
    enriched = (l1.connect(dict_stream.broadcast(DICT_DESC))
                  .process(Enrich(), output_type=ENRICHED_TYPE))
    print("[4/6] wzbogacenie wynikow L1 slownikiem (broadcast)")

    # Partia 5: narastajacy raport dzienny -> MySQL (upsert)
    attach_l2_sink(env, build_l2(enriched), cfg)
    print("[5/6] Poziom 2: raport dzienny -> MySQL")

    # Partia 6: alarmy z surowego strumienia -> gielda-alerty
    build_alerts(ticks, cfg).sink_to(build_alerts_sink(cfg))
    print("[6/6] alarmy podpiete, wysylam job na klaster")

    env.execute("gielda-partia-6")


if __name__ == "__main__":
    main()
