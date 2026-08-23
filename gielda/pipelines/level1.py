"""Level 1: 1-minute tumbling windows per (contractId, session).

Measures per the module specification:
    Miara 1 -- price range in the minute (min_price, max_price),
    Miara 2 -- total traded volume in lots (sum_lots),
    Miara 3 -- average bid-ask spread (avg_spread),
    Miara 4 -- count of ticks flagged flashCrash (flash_cnt).

All ticks participate in every measure; the only per-tick filter is the
flashCrash flag test feeding flash_cnt.
"""
from pyflink.common import Row, Types
from pyflink.common.time import Time
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

from gielda.schemas import L1_TYPE


class L1Agg(AggregateFunction):
    """Incremental aggregate over one window -- never buffers raw ticks.

    Mapping:
        Miara 1: min_price, max_price
        Miara 2: sum_lots
        Miara 3: avg_spread (sum_spread / tick_cnt)
        Miara 4: flash_cnt
        
    Accumulator layout (tuple):
        (min_price, max_price, sum_lots, sum_spread, tick_cnt, flash_cnt)
    """

    def create_accumulator(self):
        """Create the neutral accumulator for a new window.
        Returns:
            tuple: Identity values for every slot (inf/-inf for the price
            extremes so any real price replaces them).
        """
        return (float("inf"), float("-inf"), 0, 0.0, 0, 0)

    def add(self, tick, acc):
        """Fold one tick into the accumulator.

        Args:
            tick (Row): A parsed tick (schemas.TICK_TYPE).
            acc (tuple): Current accumulator.

        Returns:
            tuple: Updated accumulator.
        """
        return (min(acc[0], tick["price"]), max(acc[1], tick["price"]),
                    acc[2] + tick["volumeLots"], acc[3] + tick["spread"],
                    acc[4] + 1, acc[5] + (1 if tick["flashCrash"] else 0)
                )
    def get_result(self, acc):
        """Finalize the accumulator when the window fires.

        Args:
            acc (tuple): Final accumulator.

        Returns:
            tuple: (min_price, max_price, sum_lots, avg_spread, tick_cnt,
            flash_cnt) -- this is where sum_spread / tick_cnt becomes the
            average.
        """
        return (acc[0], acc[1], acc[2], acc[3], acc[3] / acc[4] if acc[4] > 0 else 0.0, acc[4], acc[5])

    def merge(self, a, b):
        """Merge two partial accumulators (element-wise min/max/sums).

        Args:
            a (tuple): First partial accumulator.
            b (tuple): Second partial accumulator.

        Returns:
            tuple: Combined accumulator.
        """
        return (min(a[0], b[0]), max(a[1], b[1]),
                    a[2] + b[2], a[3] + b[3],
                    a[4] + b[4], a[5] + b[5]
                )


class L1Window(ProcessWindowFunction):
    """Attaches the grouping key and window bounds to the aggregate result."""

    def process(self, key, ctx, results):
        """Emit one L1 row for a fired window.

        Args:
            key (tuple): The grouping key (contractId, session).
            ctx (ProcessWindowFunction.Context): Window metadata provider.
            results (Iterable[tuple]): Single pre-aggregated result from L1Agg.

        Yields:
            Row: One row matching schemas.L1_TYPE.
        """
        r = next(iter(results))
        yield Row(key[0], key[1], ctx.window().start, ctx.window().end, *r)


def build_l1(ticks):
    """Assemble the level 1 stage on top of the parsed tick stream.
    Tumbling window used, because the spec requires a fixed 1-minute interval 
    (non-overlapping windows).

    Args:
        ticks (DataStream): Parsed ticks with event-time watermarks assigned.

    Returns:
        DataStream: L1 minute results typed as schemas.L1_TYPE.
    """
    return (ticks
            .key_by(lambda t: (t['contractId'], t['session']),
                    key_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(L1Agg(), window_function=L1Window(), output_type=L1_TYPE))
