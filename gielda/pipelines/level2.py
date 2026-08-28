"""Poziom 2: narastajacy raport dzienny per (gielda, sesja, dzien).

Liczony WYLACZNIE ze wzbogaconego strumienia L1 - wewnetrznego, nie z tematu
Kafki, ktorego duplikaty (at-least-once) zdublowalyby liczniki.
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
    """Dzien handlowy okna, w tej samej strefie, w ktorej generator przydziela sesje."""
    return datetime.fromtimestamp(win_end_ms / 1000, tz=WARSAW).date()


class L2Report(KeyedProcessFunction):
    """Stan na klucz: dict z licznikami i szkicem (piklowany, bo szkic nie ma typu Flinka)."""

    def open(self, ctx):
        self.acc = ctx.get_state(
            ValueStateDescriptor('l2-acc', Types.PICKLED_BYTE_ARRAY()))

    def process_element(self, m, ctx):
        # emisja przy kazdym oknie = tryb narastajacy (accumulating);
        # retrakcje zbedne, bo upsert nadpisuje wiersz po kluczu glownym
        s = self.acc.value() or {'tonnes': 0.0, 'minutes': 0,
                                 'flash_minutes': 0, 'sketch': LogHistogram()}
        s['tonnes'] += m['sum_lots'] * m['lot_size_t']          # Miara 1: loty -> tony
        s['minutes'] += 1                                       # Miara 2: mianownik
        if m['flash_cnt'] >= 1:
            s['flash_minutes'] += 1                             # Miara 2: licznik
        s['sketch'].add(m['max_price'] - m['min_price'])        # Miara 3
        self.acc.update(s)

        exchange, session_f, _ = ctx.get_current_key()
        yield Row(exchange, session_f, day_of(m['win_end']),
                  s['tonnes'], s['minutes'], s['flash_minutes'],
                  s['flash_minutes'] / s['minutes'],
                  s['sketch'].quantile(0.5),
                  s['sketch'].to_json())


def build_l2(enriched):
    """Klucz (exchange, session_f, dzien): nowy dzien to nowy klucz ze swiezym stanem."""
    return (enriched
            .key_by(lambda r: (r['exchange'], r['session_f'],
                               day_of(r['win_end']).isoformat()),
                    key_type=Types.TUPLE(
                        [Types.STRING(), Types.STRING(), Types.STRING()]))
            .process(L2Report(), output_type=L2_TYPE))
