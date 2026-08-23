"""Shared type information and state descriptors.

Single source of truth for the shapes of data flowing between operators.
Keeping every descriptor here prevents two modules from declaring slightly
different types for the same stream or state.

Naming note: fields that later surface as SQL columns avoid reserved
keywords -- hence ``session_f`` in L1_TYPE (``SESSION`` is reserved in
Flink SQL) while the raw tick keeps the wire name ``session``.
"""
from pyflink.common import Types
from pyflink.datastream.state import MapStateDescriptor

#: Parsed tick -- projection of the raw JSON onto the fields the job uses.
#: ``ts`` is the event time as epoch milliseconds (converted from the
#: ISO-8601 ``timestamp`` string of the wire format).
TICK_TYPE = Types.ROW_NAMED(
    ['contractId', 'ts', 'price', 'spread', 'volumeLots', 'priceChangePct',
     'flashCrash', 'limitUp', 'limitDown', 'session'],
    [Types.STRING(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE(), Types.LONG(),
     Types.DOUBLE(), Types.BOOLEAN(), Types.BOOLEAN(), Types.BOOLEAN(), Types.STRING()])

#: Level 1 window result -- one row per (contractId, session) per 1-minute
#: window. ``win_start``/``win_end`` are epoch milliseconds.
L1_TYPE = Types.ROW_NAMED(
    ['contractId', 'session_f', 'win_start', 'win_end',
     'min_price', 'max_price', 'sum_lots', 'avg_spread', 'tick_cnt', 'flash_cnt'],
    [Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG(),
     Types.DOUBLE(), Types.DOUBLE(), Types.LONG(), Types.DOUBLE(), Types.LONG(), Types.LONG()])

#: Broadcast state holding the contract dictionary: contractId -> raw JSON
#: of the dictionary entry. State is matched by name and type, so every
#: operator must reference this one descriptor.
DICT_DESC = MapStateDescriptor('contracts', Types.STRING(), Types.STRING())
