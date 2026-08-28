"""Parsowanie tickow i watermarki."""
import json
from datetime import datetime

from pyflink.common import Duration, Row, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner


def parse_tick(raw):
    """Parsuje JSON ticka do Row (TICK_TYPE); uszkodzony rekord jest pomijany."""
    try:
        d = json.loads(raw)
        # fromisoformat w Pythonie 3.10 nie przyjmuje koncowki 'Z'
        ts = int(datetime.fromisoformat(
            d['timestamp'].replace('Z', '+00:00')
        ).timestamp() * 1000)
        yield Row(
            d['contractId'], ts, float(d['price']), float(d['spread']),
            int(d['volumeLots']), float(d['priceChangePct']), bool(d['flashCrash']),
            bool(d['limitUp']), bool(d['limitDown']), str(d['session'])
        )
    except (KeyError, ValueError, TypeError):
        # zatruty rekord nie moze wywrocic joba (petla restartow na tym samym rekordzie)
        pass


class TickTs(TimestampAssigner):
    """Czas zdarzenia z payloadu; znacznik rekordu Kafki to czas wysylki, wiec go ignorujemy."""

    def extract_timestamp(self, value, record_ts):
        return value['ts']


def build_watermarks(cfg):
    """Bounded out-of-orderness rowne disorder.max.ms generatora; idleness chroni przed pusta partycja."""
    return (WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(int(cfg["watermark.delay.sec"])))
            .with_timestamp_assigner(TickTs())
            .with_idleness(Duration.of_seconds(60)))
