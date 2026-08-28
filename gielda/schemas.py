"""Typy wierszy i deskryptory stanu - jedno zrodlo prawdy dla calego pipeline'u."""
from pyflink.common import Types
from pyflink.datastream.state import MapStateDescriptor

# sparsowany tick; ts = czas zdarzenia w ms epoch (z pola timestamp payloadu)
TICK_TYPE = Types.ROW_NAMED(
    ['contractId', 'ts', 'price', 'spread', 'volumeLots', 'priceChangePct',
     'flashCrash', 'limitUp', 'limitDown', 'session'],
    [Types.STRING(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE(), Types.LONG(),
     Types.DOUBLE(), Types.BOOLEAN(), Types.BOOLEAN(), Types.BOOLEAN(), Types.STRING()])

# wynik okna minutowego per (contractId, session);
# session_f zamiast session, bo SESSION to slowo zastrzezone w Flink SQL
L1_TYPE = Types.ROW_NAMED(
    ['contractId', 'session_f', 'win_start', 'win_end',
     'min_price', 'max_price', 'sum_lots', 'avg_spread', 'tick_cnt', 'flash_cnt'],
    [Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG(),
     Types.DOUBLE(), Types.DOUBLE(), Types.LONG(), Types.DOUBLE(), Types.LONG(), Types.LONG()])

# wynik L1 + pola slownika; dict_ok=False = wartosci domyslne (kwarantanna UNKNOWN)
ENRICHED_TYPE = Types.ROW_NAMED(
    ['contractId', 'session_f', 'win_start', 'win_end',
     'min_price', 'max_price', 'sum_lots', 'avg_spread', 'tick_cnt', 'flash_cnt',
     'commodity', 'exchange', 'currency', 'lot_size_t', 'dict_ok'],
    [Types.STRING(), Types.STRING(), Types.LONG(), Types.LONG(),
     Types.DOUBLE(), Types.DOUBLE(), Types.LONG(), Types.DOUBLE(), Types.LONG(), Types.LONG(),
     Types.STRING(), Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.BOOLEAN()])

# wiersz raportu dziennego; kolejnosc pol zgodna z DDL-em l2_out (INSERT ... SELECT *)
L2_TYPE = Types.ROW_NAMED(
    ['exchange', 'session_f', 'day', 'total_volume_t', 'minutes_cnt',
     'flash_minutes_cnt', 'flash_share', 'median_range', 'digest_state'],
    [Types.STRING(), Types.STRING(), Types.SQL_DATE(), Types.DOUBLE(), Types.LONG(),
     Types.LONG(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING()])

# slownik kontraktow w stanie rozglaszanym: contractId -> JSON wpisu
DICT_DESC = MapStateDescriptor('contracts', Types.STRING(), Types.STRING())
