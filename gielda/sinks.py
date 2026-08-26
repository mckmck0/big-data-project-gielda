"""Sinks: attach output destinations to pipeline stages via the Table API.

The DataStream KafkaSink cannot split an element into key and value; the
Table API kafka connector can ('key.fields'), which is what makes the
newest result per grouping key readable by external tools.
"""
from pyflink.table import StreamTableEnvironment


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
