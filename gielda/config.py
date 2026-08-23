"""Configuration loading and Flink environment setup.

All tunables live in application.properties (with environment-variable
overrides) so that no topic name, address or checkpoint location is
hardcoded in the job code.
"""
import configparser
import os

from pyflink.common import Configuration
from pyflink.datastream import (CheckpointingMode, ExternalizedCheckpointRetention,
                                StreamExecutionEnvironment)


def load_config(path="/opt/workspace/application.properties"):
    """Load configuration from a properties file and override with environment variables.

    The properties file has no ``[section]`` header, so a synthetic one is
    prepended before parsing. Every key can be overridden by an environment
    variable named after it, e.g. ``kafka.topic.ticks`` -> ``KAFKA_TOPIC_TICKS``.

    Args:
        path (str): Path to the properties file (read on the JobManager).

    Returns:
        dict: Configuration parameters keyed by property name.
    """
    cp = configparser.ConfigParser()
    with open(path) as f:
        cp.read_string("[cfg]\n" + f.read())
    cfg = dict(cp["cfg"])
    for k in list(cfg):
        cfg[k] = os.environ.get(k.replace(".", "_").upper(), cfg[k])
    return cfg


def get_flink_env(cfg):
    """Set up the Flink environment with checkpointing and a restart strategy.

    Checkpoints go to durable storage (MinIO/S3) at an interval below one
    minute, and the job restarts on failure by itself -- together this bounds
    the progress lost on a restart, as required by Partia 1.

    Args:
        cfg (dict): Configuration parameters from :func:`load_config`.

    Returns:
        StreamExecutionEnvironment: The configured Flink environment.
    """
    conf = Configuration()
    conf.set_string("execution.checkpointing.storage", "filesystem")
    conf.set_string("execution.checkpointing.dir", cfg["checkpoint.dir"])
    conf.set_string("state.backend.type", "hashmap")
    # Restart strategy
    conf.set_string("restart-strategy.type", "fixed-delay")
    conf.set_string("restart-strategy.fixed-delay.attempts", "10")
    conf.set_string("restart-strategy.fixed-delay.delay", "10 s")

    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.enable_checkpointing(int(cfg["checkpoint.interval.ms"]), CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_externalized_checkpoint_retention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION)

    return env
