"""Buildery zrodel Kafka (czytamy same wartosci - klucz rekordu jest tez w JSON-ie)."""
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource


def build_ticks_source(cfg):
    """Zrodlo tickow; earliest dziala tylko przy swiezym starcie, po awarii licza sie offsety z checkpointu."""
    return (KafkaSource.builder()
            .set_bootstrap_servers(cfg["kafka.bootstrap.servers"])
            .set_topics(cfg["kafka.topic.ticks"])
            .set_group_id(cfg["kafka.group.id"])
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build())


def build_dict_source(cfg):
    """Zrodlo slownika: kompaktowany temat czytany bez konca = aktualny stan + przyszle zmiany."""
    return (KafkaSource.builder()
            .set_bootstrap_servers(cfg["kafka.bootstrap.servers"])
            .set_topics(cfg["kafka.topic.dict"])
            .set_group_id(cfg["kafka.group.id"] + "-dict")
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build())
