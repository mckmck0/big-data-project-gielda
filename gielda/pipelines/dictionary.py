"""Dictionary broadcast: distributes contract definitions to all subtasks.

Partia 1 uses this stage to demonstrate live dictionary reloads on the
console; in Partia 4 the same broadcast state moves behind the level 1
window to enrich minute results (as a KeyedBroadcastProcessFunction).
"""
import json

from pyflink.datastream.functions import BroadcastProcessFunction

from gielda.schemas import DICT_DESC


class DictReader(BroadcastProcessFunction):
    """Joins the tick stream with the broadcast contract dictionary.

    The broadcast side keeps the newest dictionary entry per contractId in
    broadcast state; the data side does a read-only lookup per tick. A tick
    whose contract is missing from the dictionary is labelled explicitly
    instead of failing.
    """

    def process_broadcast_element(self, value, ctx):
        """Store or update one dictionary entry in the broadcast state.

        Args:
            value (str): A JSON string with one contract definition.
            ctx (BroadcastProcessFunction.Context): Context with writable
                broadcast state.
        """
        d = json.loads(value)
        ctx.get_broadcast_state(DICT_DESC).put(d['contractId'], value)
        print(f"[SLOWNIK] aktualizacja: {d['contractId']}")

    def process_element(self, tick, ctx):
        """Label one tick with the commodity name from the dictionary.

        Args:
            tick (Row): A parsed tick.
            ctx (BroadcastProcessFunction.ReadOnlyContext): Context with
                read-only broadcast state.

        Yields:
            str: ``"<contractId> <price> [<commodity>]"``; the label is
            ``BRAK-W-SLOWNIKU`` when the contract has no dictionary entry.
        """
        entry = ctx.get_broadcast_state(DICT_DESC).get(tick['contractId'])
        label = json.loads(entry)['commodity'] if entry else 'BRAK-W-SLOWNIKU'
        yield f"{tick['contractId']} {tick['price']} [{label}]"
