"""Poziom 1: okna 1-minutowe per (contractId, session) - cztery miary ze specyfikacji."""
from pyflink.common import Row, Types
from pyflink.common.time import Time
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows

from gielda.schemas import L1_TYPE


class L1Agg(AggregateFunction):
    """Agregacja inkrementalna; akumulator: (min, max, sum_lots, sum_spread, tick_cnt, flash_cnt).

    Miara 1: min_price/max_price, Miara 2: sum_lots,
    Miara 3: avg_spread (suma/licznik), Miara 4: flash_cnt.
    """

    def create_accumulator(self):
        # inf/-inf, zeby pierwsza realna cena podmienila ekstrema
        return (float("inf"), float("-inf"), 0, 0.0, 0, 0)

    def add(self, tick, acc):
        return (min(acc[0], tick["price"]), max(acc[1], tick["price"]),
                    acc[2] + tick["volumeLots"], acc[3] + tick["spread"],
                    acc[4] + 1, acc[5] + (1 if tick["flashCrash"] else 0)
                )

    def get_result(self, acc):
        # srednia liczona dopiero tutaj: suma+licznik sa laczne (merge dziala), srednia nie
        return (acc[0], acc[1], acc[2], acc[3] / acc[4] if acc[4] > 0 else 0.0, acc[4], acc[5])

    def merge(self, a, b):
        return (min(a[0], b[0]), max(a[1], b[1]),
                    a[2] + b[2], a[3] + b[3],
                    a[4] + b[4], a[5] + b[5]
                )


class L1Window(ProcessWindowFunction):
    """Dokleja klucz i granice okna do zagregowanego wyniku."""

    def process(self, key, ctx, results):
        r = next(iter(results))  # pojedynczy, wstepnie zagregowany wynik
        yield Row(key[0], key[1], ctx.window().start, ctx.window().end, *r)


def build_l1(ticks):
    """Tumbling 1 min (spec: stale, nienakladajace sie okna), klucz (contractId, session)."""
    # allowed_lateness celowo domyslne (0): watermark 30 s pokrywa caly kontrakt
    # generatora, a pozne ponowne odpalenia okien dublowalyby liczniki Poziomu 2
    return (ticks
            .key_by(lambda t: (t['contractId'], t['session']),
                    key_type=Types.TUPLE([Types.STRING(), Types.STRING()]))
            .window(TumblingEventTimeWindows.of(Time.minutes(1)))
            .aggregate(L1Agg(), window_function=L1Window(), output_type=L1_TYPE))
