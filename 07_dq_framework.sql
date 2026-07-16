-- ============================================================
-- Data Quality Rules
-- ============================================================

CREATE TABLE IF NOT EXISTS dbw_practice01.control.dq_rules
(
    entity_name    STRING      NOT NULL,
    rule_name      STRING      NOT NULL,
    rule_type      STRING      NOT NULL,
    column_name    STRING,

    rule_value     STRING,      -- regex, allowed values, etc.
    min_value      STRING,      -- lower bound
    max_value      STRING,      -- upper bound

    threshold      DOUBLE,
    severity       STRING      NOT NULL,
    enabled        BOOLEAN     NOT NULL
)
USING DELTA
COMMENT 'Metadata-driven Data Quality rules.';


-- ============================================================
-- Data Quality Results
-- ============================================================

CREATE TABLE IF NOT EXISTS dbw_practice01.control.dq_results
(
    run_id            STRING      NOT NULL,
    entity_name       STRING      NOT NULL,
    rule_name         STRING      NOT NULL,
    rule_type         STRING      NOT NULL,
    severity          STRING      NOT NULL,
    passed            BOOLEAN     NOT NULL,
    failed_records    BIGINT      NOT NULL,
    execution_time    TIMESTAMP   NOT NULL,
    details           STRING
)
USING DELTA
COMMENT 'Execution results of Data Quality rules.';

-- ============================================================
-- DRIVERS
-- ============================================================

INSERT INTO dbw_practice01.control.dq_rules
VALUES
(
    'drivers',
    'Duplicate Driver ID',
    'DUPLICATE_KEY',
    'driverId',
    NULL,
    NULL,
    NULL,
    0.0,
    'ERROR',
    TRUE
),

(
    'drivers',
    'Duplicate Driver Records',
    'DUPLICATE_ROW',
    NULL,
    NULL,
    NULL,
    NULL,
    0.0,
    'WARNING',
    TRUE
);


-- ============================================================
-- CONSTRUCTORS
-- ============================================================

INSERT INTO dbw_practice01.control.dq_rules
VALUES
(
    'constructors',
    'Duplicate Constructor ID',
    'DUPLICATE_KEY',
    'constructorId',
    NULL,
    NULL,
    NULL,
    0.0,
    'ERROR',
    TRUE
),

(
    'constructors',
    'Duplicate Constructor Records',
    'DUPLICATE_ROW',
    NULL,
    NULL,
    NULL,
    NULL,
    0.0,
    'WARNING',
    TRUE
);


-- ============================================================
-- CIRCUITS
-- ============================================================

INSERT INTO dbw_practice01.control.dq_rules
VALUES
(
    'circuits',
    'Duplicate Circuit ID',
    'DUPLICATE_KEY',
    'circuitId',
    NULL,
    NULL,
    NULL,
    0.0,
    'ERROR',
    TRUE
),

(
    'circuits',
    'Duplicate Circuit Records',
    'DUPLICATE_ROW',
    NULL,
    NULL,
    NULL,
    NULL,
    0.0,
    'WARNING',
    TRUE
);


-- ============================================================
-- RACES
-- ============================================================

INSERT INTO dbw_practice01.control.dq_rules
VALUES
(
    'races',
    'Duplicate Race ID',
    'DUPLICATE_KEY',
    'raceId',
    NULL,
    NULL,
    NULL,
    0.0,
    'ERROR',
    TRUE
),

(
    'races',
    'Duplicate Race Records',
    'DUPLICATE_ROW',
    NULL,
    NULL,
    NULL,
    NULL,
    0.0,
    'WARNING',
    TRUE
);


-- ============================================================
-- RESULTS
-- ============================================================

INSERT INTO dbw_practice01.control.dq_rules
VALUES
(
    'results',
    'Duplicate Result ID',
    'DUPLICATE_KEY',
    'resultId',
    NULL,
    NULL,
    NULL,
    0.0,
    'ERROR',
    TRUE
),

(
    'results',
    'Duplicate Result Records',
    'DUPLICATE_ROW',
    NULL,
    NULL,
    NULL,
    NULL,
    0.0,
    'WARNING',
    TRUE
);
