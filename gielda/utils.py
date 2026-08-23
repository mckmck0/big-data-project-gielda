"""Parsing and watermarking helpers.

Functions here run on the TaskManager's Python workers, so the module must
be shipped with the job (``env.add_python_file``) -- see main.py.
"""
import json
from datetime import datetime

from pyflink.common import Duration, Row, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner


def parse_tick(raw):
    """Parse a raw JSON tick into a Row matching ``schemas.TICK_TYPE``.

    Used with ``flat_map`` so that a malformed record yields nothing instead
    of crashing the job -- a single poison message must not put the pipeline
    into a restart loop. The ISO-8601 timestamp ends with ``Z``, which
    Python 3.10's ``fromisoformat`` cannot parse, hence the replacement.

    Args:
        raw (str): A raw JSON string representing one tick.

    Yields:
        Row: The parsed tick. Malformed records are silently dropped.
    """
    try:
        d = json.loads(raw)
        ts = int(datetime.fromisoformat(
            d['timestamp'].replace('Z', '+00:00')
        ).timestamp() * 1000)
        yield Row(
            d['contractId'], ts, float(d['price']), float(d['spread']),
            int(d['volumeLots']), float(d['priceChangePct']), bool(d['flashCrash']),
            bool(d['limitUp']), bool(d['limitDown']), str(d['session'])
        )
    except (KeyError, ValueError, TypeError):
        pass


class TickTs(TimestampAssigner):
    """Event-time extractor reading the payload timestamp of a parsed tick.

    The Kafka record timestamp is the send time and ignores the generator's
    deliberate disorder, so event time must come from the payload field.
    """

    def extract_timestamp(self, value, record_ts):
        """Extract the event time from a parsed tick Row.

        Args:
            value (Row): The parsed tick.
            record_ts (int): The Kafka record timestamp (unused).

        Returns:
            int: Event time as epoch milliseconds.
        """
        return value['ts']


def build_watermarks(cfg):
    """Build the watermark strategy for the tick stream.

    Bounded out-of-orderness equal to the generator's ``disorder.max.ms``
    (event timestamps are backdated by at most that much), plus idleness so
    a quiet partition cannot stall the overall watermark and block windows.

    Args:
        cfg (dict): Configuration parameters; uses ``watermark.delay.sec``.

    Returns:
        WatermarkStrategy: Strategy to apply on the parsed tick stream.
    """
    return (WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(int(cfg["watermark.delay.sec"])))
            .with_timestamp_assigner(TickTs())
            .with_idleness(Duration.of_seconds(60)))
