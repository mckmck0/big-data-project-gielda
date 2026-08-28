"""Ujscia: temat posredni L1, MySQL dla L2, temat alarmow."""
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.base import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import (KafkaRecordSerializationSchema,
                                                 KafkaSink)
from pyflink.table import StreamTableEnvironment


def build_alerts_sink(cfg):
    """Sink alarmow: at-least-once, bo exactly-once opoznialoby widocznosc o interwal checkpointu."""
    return (KafkaSink.builder()
            .set_bootstrap_servers(cfg["kafka.bootstrap.servers"])
            .set_record_serializer(KafkaRecordSerializationSchema.builder()
                                   .set_topic(cfg["kafka.topic.alerts"])
                                   .set_value_serialization_schema(SimpleStringSchema())
                                   .build())
            .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build())


def attach_l1_sink(env, l1, cfg):
    """Temat posredni przez Table API (kluczowanie rekordow via key.fields); at-least-once wg rubryki."""
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
    ss.attach_as_datastream()  # wpina INSERT z powrotem do grafu tego samego joba


def l2_sink_ddl(cfg, table_name="l2_out"):
    """DDL ujscia MySQL; PRIMARY KEY przelacza sink w tryb upsert (idempotentne powtorki po awarii)."""
    # updated_at celowo pominiete - utrzymuje je MySQL;
    # flush 1 wiersz / 1 s = wyniki widoczne od razu
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
    """Finalne ujscie raportu dziennego: MySQL, upsert po kluczu glownym."""
    t_env = StreamTableEnvironment.create(env)
    t_env.execute_sql(l2_sink_ddl(cfg))
    t_env.create_temporary_view("l2_v", t_env.from_data_stream(l2))
    ss = t_env.create_statement_set()
    ss.add_insert_sql("INSERT INTO l2_out SELECT * FROM l2_v")
    ss.attach_as_datastream()
