"""Wzbogacanie wynikow L1 slownikiem ze stanu rozglaszanego (Partie 1 i 4)."""
import json

from pyflink.common import Row
from pyflink.datastream.functions import BroadcastProcessFunction

from gielda.schemas import DICT_DESC


class Enrich(BroadcastProcessFunction):
    """Doklada pola slownika do wyniku L1; brak wpisu = wartosci domyslne, nigdy wyjatek."""

    def process_broadcast_element(self, value, ctx):
        # marker {"deleted": true} zastepuje tombstone (pusta wartosc psulaby deserializacje)
        try:
            d = json.loads(value)
            state = ctx.get_broadcast_state(DICT_DESC)
            if d.get('deleted'):
                state.remove(d['contractId'])
                print(f"[SLOWNIK] usuniecie wpisu: {d['contractId']}")
            else:
                state.put(d['contractId'], value)
                print(f"[SLOWNIK] aktualizacja: {d['contractId']}")
        except (ValueError, KeyError, TypeError) as e:
            # zatruty rekord na strumieniu kontrolnym tez nie moze polozyc joba
            print(f"[SLOWNIK][WARN] pominieto niepoprawny rekord slownika: {e}")

    def process_element(self, l1row, ctx):
        entry = ctx.get_broadcast_state(DICT_DESC).get(l1row['contractId'])
        if entry:
            d = json.loads(entry)
            yield Row(*l1row, str(d.get('commodity', 'UNKNOWN')),
                      str(d.get('exchange', 'UNKNOWN')),
                      str(d.get('currency', 'UNKNOWN')),
                      float(d.get('lotSizeT', 0.0)), True)
        else:
            # kwarantanna: UNKNOWN laduje w osobnej grupie L2 zamiast psuc MATIF/CBOT
            print(f"[SLOWNIK][WARN] brak wpisu dla {l1row['contractId']} "
                  f"(okno konczace sie {l1row['win_end']}) - wartosci domyslne")
            yield Row(*l1row, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 0.0, False)
