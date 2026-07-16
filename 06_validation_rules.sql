-- DROP TABLE IF EXISTS dbw_practice01.control.validation_rules;

CREATE TABLE dbw_practice01.control.validation_rules (
    entity_name STRING NOT NULL,
    rule_name   STRING NOT NULL,
    rule_type   STRING NOT NULL,
    column_name STRING,
    rule_value  STRING,
    severity    STRING NOT NULL,
    enabled     BOOLEAN NOT NULL
)
USING DELTA
COMMENT 'Metadata-driven validation rules for ingestion framework.';


-- ============================================================
-- Drivers
-- Actual columns: dateOfBirth, driverId, name (struct), nationality, url
-- ============================================================

INSERT INTO dbw_practice01.control.validation_rules VALUES
('drivers', 'Driver ID Required',      'NOT_NULL', 'driverId',    NULL, 'ERROR', TRUE),
('drivers', 'Driver Name Required',    'NOT_NULL', 'name',        NULL, 'ERROR', TRUE),
('drivers', 'Nationality Required',    'NOT_NULL', 'nationality', NULL, 'ERROR', TRUE);


-- ============================================================
-- Constructors
-- Actual columns: constructorId, name, nationality, url
-- ============================================================

INSERT INTO dbw_practice01.control.validation_rules VALUES
('constructors', 'Constructor ID Required',   'NOT_NULL', 'constructorId', NULL, 'ERROR', TRUE),
('constructors', 'Constructor Name Required', 'NOT_NULL', 'name',          NULL, 'ERROR', TRUE),
('constructors', 'Nationality Required',      'NOT_NULL', 'nationality',   NULL, 'ERROR', TRUE);


-- ============================================================
-- Circuits
-- Actual columns: circuitId, url, circuitName, lat, long, locality, country
-- ============================================================

INSERT INTO dbw_practice01.control.validation_rules VALUES
('circuits', 'Circuit ID Required',   'NOT_NULL', 'circuitId',   NULL, 'ERROR', TRUE),
('circuits', 'Circuit Name Required', 'NOT_NULL', 'circuitName', NULL, 'ERROR', TRUE),
('circuits', 'Locality Required',     'NOT_NULL', 'locality',    NULL, 'ERROR', TRUE),
('circuits', 'Country Required',      'NOT_NULL', 'country',     NULL, 'ERROR', TRUE);


-- ============================================================
-- Races
-- Actual columns: season, round, url, raceName, date, circuitId
-- ============================================================

INSERT INTO dbw_practice01.control.validation_rules VALUES
('races', 'Season Required',    'NOT_NULL', 'season',    NULL, 'ERROR', TRUE),
('races', 'Round Required',     'NOT_NULL', 'round',     NULL, 'ERROR', TRUE),
('races', 'Circuit ID Required','NOT_NULL', 'circuitId', NULL, 'ERROR', TRUE),
('races', 'Race Name Required', 'NOT_NULL', 'raceName',  NULL, 'ERROR', TRUE),
('races', 'Race Date Required', 'NOT_NULL', 'date',      NULL, 'ERROR', TRUE);


-- ============================================================
-- Results
-- Actual columns: constructorId, date, driverId, grid, laps, number,
--                 points, position, positionText, raceName, round, season, status, url
-- ============================================================

INSERT INTO dbw_practice01.control.validation_rules VALUES
('results', 'Driver ID Required',      'NOT_NULL', 'driverId',      NULL, 'ERROR', TRUE),
('results', 'Constructor ID Required', 'NOT_NULL', 'constructorId', NULL, 'ERROR', TRUE),
('results', 'Grid Required',           'NOT_NULL', 'grid',          NULL, 'ERROR', TRUE),
('results', 'Position Required',       'NOT_NULL', 'position',      NULL, 'ERROR', TRUE),
('results', 'Points Required',         'NOT_NULL', 'points',        NULL, 'ERROR', TRUE);
