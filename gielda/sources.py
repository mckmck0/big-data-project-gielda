"""Kafka source builders.

Both sources deserialize values only -- the record key (contractId) is also
present inside the JSON payload, so nothing is lost.
"""
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource


def build_ticks_source(cfg):
    """Build the source for the raw tick stream.

    Starts from the earliest offset for reproducible runs during
    development; on recovery Flink resumes from the offsets stored in the
    checkpoint, not from this initializer.

    Args:
        cfg (dict): Configuration parameters.

    Returns:
        KafkaSource: Source reading raw JSON strings from the tick topic.
    """
    return (KafkaSource.builder()
            .set_bootstrap_servers(cfg["kafka.bootstrap.servers"])
            .set_topics(cfg["kafka.topic.ticks"])
            .set_group_id(cfg["kafka.group.id"])
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build())


def build_dict_source(cfg):
    """Build the source for the contract dictionary topic.

    The topic is compacted and read from the earliest offset as an unbounded
    stream, so a running job receives both the current dictionary state and
    every future update -- this is what makes the dictionary reloadable
    without a restart.

    Args:
        cfg (dict): Configuration parameters.

    Returns:
        KafkaSource: Source reading raw JSON dictionary entries.
    """
    return (KafkaSource.builder()
            .set_bootstrap_servers(cfg["kafka.bootstrap.servers"])
            .set_topics(cfg["kafka.topic.dict"])
            .set_group_id(cfg["kafka.group.id"] + "-dict")
            .set_starting_offsets(KafkaOffsetsInitializer.earliest())
            .set_value_only_deserializer(SimpleStringSchema())
            .build())
