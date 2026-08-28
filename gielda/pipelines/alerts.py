"""Alarmy (Partia 6): natychmiastowy (limit up/down) i licznikowy (flash crash).

Obie galezie schodza z surowego strumienia tickow, przed jakakolwiek
agregacja - ta galaz ma jeszcze watermarki (odgalezia sie przed broadcastem).
"""
import json
from datetime import datetime, timezone

from pyflink.common import Types
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor, ValueStateDescriptor


def _iso(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def to_limit_alert(tick):
    """Rekord alarmu natychmiastowego: typ, klucz, czas zdarzenia, wartosci wyzwalajace."""
    return json.dumps({
        'alertType': 'LIMIT_UP' if tick['limitUp'] else 'LIMIT_DOWN',
        'contractId': tick['contractId'],
        'eventTime': _iso(tick['ts']),
        'price': tick['price'],
        'priceChangePct': tick['priceChangePct'],
    })


class FlashPatternDetector(KeyedProcessFunction):
    """Wlasne okno przesuwne: >= threshold tickow flashCrash w dowolnych window_min minutach.

    Okno przesuwa sie z kazdym zdarzeniem (nie skokowo co minute), alarm
    wychodzi natychmiast przy przekroczeniu progu, jeden alarm na epizod.
    """

    def __init__(self, threshold, window_min):
        self.threshold = threshold
        self.window_ms = window_min * 60 * 1000
        self.flash_ts = None
        self.alerted = None

    def open(self, ctx):
        self.flash_ts = ctx.get_list_state(
            ListStateDescriptor('flash-ts', Types.LONG()))
        self.alerted = ctx.get_state(
            ValueStateDescriptor('alerted', Types.BOOLEAN()))

    def process_element(self, tick, ctx):
        ts = tick['ts']
        # o przynaleznosci do okna decyduje czas zdarzenia, nie kolejnosc
        # dotarcia - spozniony tick tez domyka wzorzec
        stamps = list(self.flash_ts.get()) + [ts]
        newest = max(stamps)
        stamps = sorted(t for t in stamps if t > newest - self.window_ms)
        self.flash_ts.update(stamps)
        ctx.timer_service().register_event_time_timer(ts + self.window_ms)

        # flaga tlumi duplikaty w ramach epizodu; zbroi sie ponownie w on_timer
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
        # przycina stare znaczniki i czysci stan, gdy anomalie ustana
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
    """Alarm natychmiastowy: goly filter+map (zero stanu i okien). Licznikowy: detektor per kontrakt."""
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
