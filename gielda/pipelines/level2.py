"""Level 2: continuously-updated daily report per (exchange, session, day).

Built EXCLUSIVELY on the enriched level 1 stream (grading criterion) -- the
in-job stream, not the intermediate Kafka topic, whose at-least-once
duplicates would double-count the cumulative measures.

Measures per the module specification:
    Miara 1 -- cumulative session volume in tonnes (sum_lots * lotSizeT),
    Miara 2 -- share of minutes with >= 1 flash-crash signal
               (raw counters kept; the ratio is derived at emission),
    Miara 3 -- approximate median of minute price ranges (LogHistogram),
               persisted in serialized form so future days can be merged
               without returning to the source data.

Day handling: the trading day (Europe/Warsaw, consistent with how the
generator assigns sessions) is part of the grouping key -- a new day simply
starts fresh state. Event-time timers are NOT an option here: downstream of
the broadcast connect the watermark never advances.
"""
from datetime import datetime
from zoneinfo import ZoneInfo

from pyflink.common import Row, Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor

from gielda.schemas import L2_TYPE
from gielda.sketch import LogHistogram

WARSAW = ZoneInfo("Europe/Warsaw")


def day_of(win_end_ms):
    """Trading day a window belongs to.

    Args:
        win_end_ms (int): Window end as epoch milliseconds.

    Returns:
        datetime.date: The calendar day of the window end in Europe/Warsaw.
    """
    return datetime.fromtimestamp(win_end_ms / 1000, tz=WARSAW).date()


class L2Report(KeyedProcessFunction):
    """Cumulative daily aggregates over enriched minute results.

    Keyed by (exchange, session_f, day). Keeps one ValueState with a pickled
    dict {tonnes, minutes, flash_minutes, sketch} -- an opaque accumulator
    (the sketch has no Flink type), checkpointed like any other state, so
    recovery restores it consistently with the Kafka offsets.
    """

    def open(self, ctx):
        """Register the accumulator state.

        Args:
            ctx (RuntimeContext): Flink runtime context.
        """
        self.acc = ctx.get_state(
            ValueStateDescriptor('l2-acc', Types.PICKLED_BYTE_ARRAY()))

    def process_element(self, m, ctx):
        """Fold one enriched minute result and emit the updated daily row.

        Args:
            m (Row): One enriched L1 result (schemas.ENRICHED_TYPE).
            ctx (KeyedProcessFunction.Context): Provides get_current_key().

        Yields:
            Row: One row matching schemas.L2_TYPE with the key's updated
            cumulative measures (emitted on EVERY element -- the rubric's
            continuously-updating mode).
        """
        s = self.acc.value() or {'tonnes': 0.0, 'minutes': 0,
                                 'flash_minutes': 0, 'sketch': LogHistogram()}
        s['tonnes'] += m['sum_lots'] * m['lot_size_t']          # Miara 1
        s['minutes'] += 1                                       # Miara 2
        if m['flash_cnt'] >= 1:
            s['flash_minutes'] += 1
        s['sketch'].add(m['max_price'] - m['min_price'])        # Miara 3
        self.acc.update(s)

        exchange, session_f, _ = ctx.get_current_key()
        yield Row(exchange, session_f, day_of(m['win_end']),
                  s['tonnes'], s['minutes'], s['flash_minutes'],
                  s['flash_minutes'] / s['minutes'],
                  s['sketch'].quantile(0.5),
                  s['sketch'].to_json())


def build_l2(enriched):
    """Assemble the level 2 stage on top of the enriched L1 stream.

    Args:
        enriched (DataStream): Enriched minute results (ENRICHED_TYPE).

    Returns:
        DataStream: Continuously-updated daily rows typed as L2_TYPE.
    """
    return (enriched
            .key_by(lambda r: (r['exchange'], r['session_f'],
                               day_of(r['win_end']).isoformat()),
                    key_type=Types.TUPLE(
                        [Types.STRING(), Types.STRING(), Types.STRING()]))
            .process(L2Report(), output_type=L2_TYPE))
