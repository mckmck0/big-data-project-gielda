"""Dictionary broadcast: enriches level 1 results with contract context.

The compacted topic gielda-kontrakty is read as an unbounded stream and
broadcast to every subtask, so a running job picks up dictionary changes
without a restart (Partia 1 criterion, demonstrated end-to-end in Partia 4).
"""
import json

from pyflink.common import Row
from pyflink.datastream.functions import BroadcastProcessFunction

from gielda.schemas import DICT_DESC


class Enrich(BroadcastProcessFunction):
    """Enriches level 1 minute results with dictionary context (Partia 4).

    Broadcast side maintains the contract dictionary (including deletions
    via a ``{"deleted": true}`` marker -- the JSON stand-in for a compacted
    topic's tombstone); data side extends each L1 row with commodity,
    exchange, currency and lotSizeT. A contract without a dictionary entry
    yields defaults with ``dict_ok=False`` and a logged warning -- never an
    exception (graded: no silent NullPointerException).
    """

    def process_broadcast_element(self, value, ctx):
        """Apply one dictionary update (upsert or deletion) to the state.

        A malformed dictionary record is logged and skipped -- a poison
        message on the control stream must not take the pipeline down.

        Args:
            value (str): JSON with one contract definition, or a deletion
                marker ``{"contractId": ..., "deleted": true}``.
            ctx (BroadcastProcessFunction.Context): Context with writable
                broadcast state.
        """
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
            print(f"[SLOWNIK][WARN] pominieto niepoprawny rekord slownika: {e}")

    def process_element(self, l1row, ctx):
        """Extend one L1 row with the four dictionary fields.

        Args:
            l1row (Row): A level 1 minute result (schemas.L1_TYPE).
            ctx (BroadcastProcessFunction.ReadOnlyContext): Context with
                read-only broadcast state.

        Yields:
            Row: One row matching schemas.ENRICHED_TYPE -- the 10 L1 fields
            followed by commodity, exchange, currency, lot_size_t, dict_ok.
        """
        entry = ctx.get_broadcast_state(DICT_DESC).get(l1row['contractId'])
        if entry:
            d = json.loads(entry)
            yield Row(*l1row, str(d.get('commodity', 'UNKNOWN')),
                      str(d.get('exchange', 'UNKNOWN')),
                      str(d.get('currency', 'UNKNOWN')),
                      float(d.get('lotSizeT', 0.0)), True)
        else:
            print(f"[SLOWNIK][WARN] brak wpisu dla {l1row['contractId']} "
                  f"(okno konczace sie {l1row['win_end']}) - wartosci domyslne")
            yield Row(*l1row, 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 0.0, False)
