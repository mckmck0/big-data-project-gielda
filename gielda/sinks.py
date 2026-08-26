"""Sinks: output destinations for pipeline stages.

Level 1/2 go through the Table API (the DataStream KafkaSink cannot split
an element into key and value, and JDBC upserts need SQL DDL); the alert
stream uses a plain DataStream KafkaSink for minimal latency.
"""
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (KafkaRecordSerializationSchema,
                                                 KafkaSink)
from pyflink.table import StreamTableEnvironment


def build_alerts_sink(cfg):
    """Build the Kafka sink for alert records (Partia 6).

    AT_LEAST_ONCE by design: alerts become visible the moment they are
    written, instead of waiting up to a checkpoint interval for a
    transaction commit (EXACTLY_ONCE would add latency the alarm criteria
    penalize). Possible post-recovery duplicates are acceptable for alerts
    and deduplicable by (alertType, contractId, eventTime).

    Args:
        cfg (dict): Configuration parameters.

    Returns:
        KafkaSink: Sink writing alert JSON strings to the alert topic.
    """
    return (KafkaSink.builder()
            .set_bootstrap_servers(cfg["kafka.bootstrap.servers"])
            .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                   .set_topic(cfg["kafka.topic.alerts"])
                                   .set_value_serialization_schema(SimpleStringSchema())
                                   .build())
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build())


def attach_l1_sink(env, l1, cfg):
    """Attach a keyed JSON Kafka sink for level 1 results to the job graph.

    Delivery is at-least-once by design: the rubric requires exactly-once
    only at the final sink, and the level 2 upserts absorb any duplicates.

    Args:
        env (StreamExecutionEnvironment): The job's environment.
        l1 (DataStream): Level 1 results typed as schemas.L1_TYPE.
        cfg (dict): Configuration parameters.
    """
    t_env = StreamTableEnvironment.create(env)
    t_env.execute_sql(f"""
        CREATE TABLE l1_out (
            contractId STRING, session_f STRING, win_start BIGINT, win_end BIGINT,
            min_price DOUBLE, max_price DOUBLE, sum_lots BIGINT,
            avg_spread DOUBLE, tick_cnt BIGINT, flash_cnt BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{cfg["kafka.topic.l1"]}',
            'properties.bootstrap.servers' = '{cfg["kafka.bootstrap.servers"]}',
            'format' = 'json',
            'key.format' = 'json',
            'key.fields' = 'contractId;session_f',
            'sink.delivery-guarantee' = 'at-least-once'
        )
    """)
    t_env.create_temporary_view("l1_v", t_env.from_data_stream(l1))
    ss = t_env.create_statement_set()
    ss.add_insert_sql("INSERT INTO l1_out SELECT * FROM l1_v")
    ss.attach_as_datastream()   # folds the INSERT back into this job's graph


def l2_sink_ddl(cfg, table_name="l2_out"):
    """Build the Flink DDL for the final MySQL sink of level 2 results.

    The declared PRIMARY KEY switches the JDBC sink into upsert mode -- the
    MySQL dialect writes INSERT ... ON DUPLICATE KEY UPDATE, so checkpoint
    replays rewrite the same keys idempotently (effectively exactly-once).
    ``updated_at`` is deliberately absent: MySQL maintains it itself.
    Flush options are tuned for freshness ("wyniki dostepne najszybciej jak
    to mozliwe"): every row is written immediately, which at a few updates
    per minute costs nothing.

    Args:
        cfg (dict): Configuration parameters (mysql.* keys).
        table_name (str): Name to register the Flink-side table under.

    Returns:
        str: A CREATE TABLE statement for ``t_env.execute_sql``.
    """
    return f"""
        CREATE TABLE {table_name} (
            exchange          STRING,
            session_f         STRING,
            `day`             DATE,
            total_volume_t    DOUBLE,
            minutes_cnt       BIGINT,
            flash_minutes_cnt BIGINT,
            flash_share       DOUBLE,
            median_range      DOUBLE,
            digest_state      STRING,
            PRIMARY KEY (exchange, session_f, `day`) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{cfg["mysql.url"]}',
            'table-name' = '{cfg["mysql.table.l2"]}',
            'username' = '{cfg["mysql.user"]}',
            'password' = '{cfg["mysql.password"]}',
            'sink.buffer-flush.max-rows' = '1',
            'sink.buffer-flush.interval' = '1s'
        )
    """


def attach_l2_sink(env, l2, cfg):
    """Attach the final MySQL upsert sink for level 2 results (Partia 5).

    Args:
        env (StreamExecutionEnvironment): The job's environment.
        l2 (DataStream): Level 2 report rows typed as schemas.L2_TYPE.
        cfg (dict): Configuration parameters.
    """
    t_env = StreamTableEnvironment.create(env)
    t_env.execute_sql(l2_sink_ddl(cfg))
    t_env.create_temporary_view("l2_v", t_env.from_data_stream(l2))
    ss = t_env.create_statement_set()
    ss.add_insert_sql("INSERT INTO l2_out SELECT * FROM l2_v")
    ss.attach_as_datastream()
