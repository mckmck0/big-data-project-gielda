"""Alerts (Partia 6): immediate limit alarms and the flash-crash pattern.

Both alarms consume the RAW tick stream (not level 1 results): the immediate
alarm must not wait for any window, and the pattern alarm is defined on
individual ticks. This branch still carries event-time watermarks -- it
splits off before the broadcast connect that freezes them.

Latency characteristics (grading criteria):
    * immediate alarm  -- filter + map only, no windows/keying/state; delay
      is just the pipeline's processing latency,
    * pattern alarm    -- fires when the watermark passes a sliding window's
      end, i.e. window close + the out-of-orderness delay (30 s); that lag
      is inherent to correctly handling disordered events.
"""
import json
from datetime import datetime, timezone

from pyflink.common import Types
from pyflink.common.time import Time
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import SlidingEventTimeWindows


def _iso(ms):
    """Format epoch milliseconds as an ISO-8601 UTC string."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def to_limit_alert(tick):
    """Build the immediate alert record for a limit-up/limit-down tick.

    Args:
        tick (Row): A parsed tick with limitUp or limitDown set.

    Returns:
        str: JSON with alert type, key, event time and triggering values.
    """
    return json.dumps({
        'alertType': 'LIMIT_UP' if tick['limitUp'] else 'LIMIT_DOWN',
        'contractId': tick['contractId'],
        'eventTime': _iso(tick['ts']),
        'price': tick['price'],
        'priceChangePct': tick['priceChangePct'],
    })


class FlashCount(AggregateFunction):
    """Incremental count of flash-crash ticks inside one sliding window."""

    def create_accumulator(self):
        return 0

    def add(self, tick, acc):
        return acc + 1

    def get_result(self, acc):
        return acc

    def merge(self, a, b):
        return a + b


class FlashPatternAlert(ProcessWindowFunction):
    """Emits an alert when a window's flash-crash count reaches the threshold.

    Args:
        threshold (int): Minimum flash-crash ticks in the window to alarm.
    """

    def __init__(self, threshold):
        self.threshold = threshold

    def process(self, key, ctx, results):
        """Yield one alert record if the pattern condition holds.

        Args:
            key (str): The contractId this window belongs to.
            ctx (ProcessWindowFunction.Context): Window metadata provider.
            results (Iterable[int]): Single pre-aggregated count.

        Yields:
            str: JSON alert record; nothing when the count is below the
            threshold (sub-threshold windows produce no output at all).
        """
        n = next(iter(results))
        if n >= self.threshold:
            yield json.dumps({
                'alertType': 'FLASH_CRASH_PATTERN',
                'contractId': key,
                'windowStart': _iso(ctx.window().start),
                'windowEnd': _iso(ctx.window().end),
                'flashCount': n,
            })


def build_alerts(ticks, cfg):
    """Assemble both alarm streams and union them into one alert stream.

    Args:
        ticks (DataStream): Parsed ticks with event-time watermarks.
        cfg (dict): Configuration (alert.flash.count, alert.flash.window.min).

    Returns:
        DataStream: JSON alert records (both alarm types) as strings.
    """
    immediate = (ticks
                 .filter(lambda t: t['limitUp'] or t['limitDown'])
                 .map(to_limit_alert, output_type=Types.STRING()))

    pattern = (ticks
               .filter(lambda t: t['flashCrash'])
               .key_by(lambda t: t['contractId'], key_type=Types.STRING())
               .window(SlidingEventTimeWindows.of(
                   Time.minutes(int(cfg['alert.flash.window.min'])),
                   Time.minutes(1)))
               .aggregate(FlashCount(),
                          window_function=FlashPatternAlert(int(cfg['alert.flash.count'])),
                          output_type=Types.STRING()))

    return immediate.union(pattern)
