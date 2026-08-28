"""Konfiguracja aplikacji i srodowiska Flink."""
import configparser
import os

from pyflink.common import Configuration
from pyflink.datastream import (CheckpointingMode, ExternalizedCheckpointRetention,
                                StreamExecutionEnvironment)


def load_config(path="/opt/workspace/application.properties"):
    """Wczytuje application.properties; kazdy klucz mozna nadpisac zmienna srodowiskowa."""
    cp = configparser.ConfigParser()
    with open(path) as f:
        cp.read_string("[cfg]\n" + f.read())  # plik .properties nie ma naglowka sekcji
    cfg = dict(cp["cfg"])
    for k in list(cfg):
        # kafka.topic.ticks -> KAFKA_TOPIC_TICKS
        cfg[k] = os.environ.get(k.replace(".", "_").upper(), cfg[k])
    return cfg


def get_flink_env(cfg):
    """Srodowisko z checkpointingiem exactly-once (S3/MinIO) i strategia restartow."""
    conf = Configuration()
    conf.set_string("execution.checkpointing.storage", "filesystem")
    conf.set_string("execution.checkpointing.dir", cfg["checkpoint.dir"])
    conf.set_string("state.backend.type", "hashmap")
    # limit prob celowy: prawdziwy blad ma polozyc job, a nie krecic petle restartow
    conf.set_string("restart-strategy.type", "fixed-delay")
    conf.set_string("restart-strategy.fixed-delay.attempts", "10")
    conf.set_string("restart-strategy.fixed-delay.delay", "10 s")

    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.enable_checkpointing(int(cfg["checkpoint.interval.ms"]), CheckpointingMode.EXACTLY_ONCE)
    # checkpoint zostaje po recznym cancelu -> wznowienie przez flink run -s
    env.get_checkpoint_config().set_externalized_checkpoint_retention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)
    return env
