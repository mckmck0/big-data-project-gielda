"""Przetwarzanie strumienia gieldy towarowej (modul generator-gielda).

config     - konfiguracja i srodowisko Flink
schemas    - typy wierszy i deskryptory stanu
utils      - parsowanie i watermarki
sources    - zrodla Kafka
sinks      - ujscia (temat posredni, MySQL, alarmy)
sketch     - szkic kwantylowy do mediany
pipelines  - etapy przetwarzania (L1, slownik, L2, alarmy)
"""
