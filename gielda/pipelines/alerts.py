"""Alerts (Partia 6): immediate limit alarms and the flash-crash pattern.

Both alarms consume the RAW tick stream (not level 1 results): the immediate
alarm must not wait for any window, and the pattern alarm is defined on
individual ticks. This branch still carries event-time watermarks -- it
splits off before the broadcast connect that freezes them.

Latency characteristics (grading criteria):
    * immediate alarm  -- filter + map only, no windows/keying/state; delay
      is just the pipeline's processing latency,
    * pattern alarm    -- a hand-rolled crawling window (KeyedProcessFunction
      + ListState of event timestamps): the alarm fires the moment the
      third flash tick ARRIVES, without waiting for any window to close.
      This is both faster than a sliding window and faithful to the spec's
      "any 5 consecutive minutes" (a 1-minute slide would quantize window
      starts and miss patterns straddling minute boundaries), and it emits
      ONE alert per episode instead of one per overlapping window.
"""
import json
from datetime import datetime, timezone

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor


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


class FlashPatternDetector(KeyedProcessFunction):
    """Alarms when >= threshold flash-crash ticks fall in any 5-minute span.

    Keyed by contractId; sees only flashCrash ticks. State per key:
        * ListState of event timestamps observed within the current span,
        * ValueState flag suppressing duplicate alerts for one episode --
          it resets once pruning drops the count below the threshold.

    Pruning keeps timestamps within (newest - window, newest], so a late
    (out-of-order) tick still counts toward the pattern -- correctness under
    disorder comes from using event timestamps, not arrival order. An
    event-time timer per tick (ts + window) prunes the state when the flash
    flow stops, so quiet keys do not hold stale state forever.

    Args:
        threshold (int): Minimum flash ticks in the span to alarm.
        window_min (int): Span length in minutes.
    """

    def __init__(self, threshold, window_min):
        self.threshold = threshold
        self.window_ms = window_min * 60 * 1000
        self.flash_ts = None
        self.alerted = None

    def open(self, ctx):
        """Register the per-key state handles.

        Args:
            ctx (RuntimeContext): Flink runtime context.
        """
        self.flash_ts = ctx.get_list_state(
            ListStateDescriptor('flash-ts', Types.LONG()))
        self.alerted = ctx.get_state(
            ValueStateDescriptor('alerted', Types.BOOLEAN()))

    def process_element(self, tick, ctx):
        """Fold one flash tick into the span and alarm on threshold crossing.

        Args:
            tick (Row): A parsed tick with flashCrash set.
            ctx (KeyedProcessFunction.Context): Timer service provider.

        Yields:
            str: JSON alert record, only when the count CROSSES the
            threshold (one alert per episode, no duplicates).
        """
        ts = tick['ts']
        stamps = list(self.flash_ts.get()) + [ts]
        newest = max(stamps)
        stamps = sorted(t for t in stamps if t > newest - self.window_ms)
        self.flash_ts.update(stamps)
        ctx.timer_service().register_event_time_timer(ts + self.window_ms)

        if len(stamps) >= self.threshold and not (self.alerted.value() or False):
            self.alerted.update(True)
            yield json.dumps({
                'alertType': 'FLASH_CRASH_PATTERN',
                'contractId': ctx.get_current_key(),
                'windowStart': _iso(stamps[0]),
                'windowEnd': _iso(newest),
                'flashCount': len(stamps),
            })

    def on_timer(self, timestamp, ctx):
        """Prune expired timestamps; re-arm alerting once the episode ends.

        Args:
            timestamp (int): The firing timer's event-time timestamp.
            ctx (KeyedProcessFunction.OnTimerContext): Timer context.
        """
        stamps = [t for t in self.flash_ts.get() if t > timestamp - self.window_ms]
        if stamps:
            self.flash_ts.update(stamps)
            if len(stamps) < self.threshold:
                self.alerted.update(False)
        else:
            self.flash_ts.clear()
            self.alerted.clear()
        return iter(())


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
               .process(FlashPatternDetector(int(cfg['alert.flash.count']),
                                             int(cfg['alert.flash.window.min'])),
                        output_type=Types.STRING()))

    return immediate.union(pattern)
