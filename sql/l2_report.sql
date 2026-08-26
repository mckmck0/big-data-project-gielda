-- Finalne ujscie: dzienny raport Poziomu 2 per (gielda, faza sesji, dzien).
--
-- Zasady projektowe (Partia 3):
--  * PK = klucz grupowania L2 + dzien -> zapis w trybie upsert
--    (INSERT ... ON DUPLICATE KEY UPDATE) = zapis idempotentny; wraz
--    z checkpointami Flinka daje efektywnie exactly-once na ujsciu.
--  * Surowe liczniki (minutes_cnt, flash_minutes_cnt) zamiast samej proporcji:
--    proporcje z wielu dob nie sumuja sie, liczniki tak.
--  * digest_state: serializowany szkic kwantylowy (log-histogram w JSON),
--    scalanie wynikow kolejnych dob bez powrotu do danych zrodlowych.
--  * updated_at utrzymuje MySQL - kolumna nie jest zapisywana przez Flinka.
--
-- Wykonanie:
--   docker exec -i mysql mysql -uroot -ppassword lab_db < sql/l2_report.sql

CREATE TABLE IF NOT EXISTS l2_report (
    exchange          VARCHAR(16)  NOT NULL,
    session_f         VARCHAR(16)  NOT NULL,
    day               DATE         NOT NULL,
    total_volume_t    DOUBLE       NOT NULL,  -- Miara 1: narastajaca suma lots * lotSizeT
    minutes_cnt       BIGINT       NOT NULL,  -- Miara 2: mianownik (zamkniete okna minutowe)
    flash_minutes_cnt BIGINT       NOT NULL,  -- Miara 2: licznik (okna z flash_cnt >= 1)
    flash_share       DOUBLE       NOT NULL,  -- Miara 2: wygoda odczytu
    median_range      DOUBLE,                 -- Miara 3: wygoda odczytu
    digest_state      MEDIUMTEXT   NOT NULL,  -- Miara 3: szkic (mergowalny!)
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (exchange, session_f, day)
);

-- Przyklad agregacji miedzy dobami bez powrotu do Kafki (uzasadnienie schematu):
--   SELECT exchange, session_f,
--          SUM(total_volume_t)                          AS wolumen_laczny,
--          SUM(flash_minutes_cnt) / SUM(minutes_cnt)    AS udzial_flash
--   FROM l2_report GROUP BY exchange, session_f;
-- (mediane z wielu dob scala sie w Pythonie: LogHistogram.merge na digest_state)
