--
-- LIMIT/OFFSET regression coverage
--

CREATE TEMP TABLE limit_tbl (
    id INTEGER,
    label TEXT
);

INSERT INTO limit_tbl VALUES
    (1, 'a'),
    (2, 'b'),
    (3, 'c'),
    (4, 'd'),
    (5, 'e'),
    (6, 'f');

-- LIMIT 0 should return no rows but still be valid
SELECT id, label FROM limit_tbl ORDER BY id LIMIT 0;

-- LIMIT ALL should behave like no limit
SELECT id, label FROM limit_tbl ORDER BY id LIMIT ALL;

-- OFFSET without LIMIT
SELECT id, label FROM limit_tbl ORDER BY id OFFSET 3;

-- LIMIT + OFFSET
SELECT id, label FROM limit_tbl ORDER BY id LIMIT 2 OFFSET 2;

-- Simple edge shape with descending sort
SELECT id, label FROM limit_tbl ORDER BY id DESC LIMIT 1;

DROP TABLE limit_tbl;
