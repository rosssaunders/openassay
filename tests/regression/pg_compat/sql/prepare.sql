--
-- PREPARE/EXECUTE/DEALLOCATE regression coverage
--

CREATE TEMP TABLE prepare_tbl (
    id INTEGER,
    note TEXT
);

-- Named prepared statement lifecycle
PREPARE prep_select AS SELECT 1 AS one;
EXECUTE prep_select;
DEALLOCATE prep_select;

-- Parameterized prepared statement lifecycle
PREPARE prep_insert (integer, text) AS
    INSERT INTO prepare_tbl VALUES ($1, $2);
EXECUTE prep_insert(1, 'alpha');
EXECUTE prep_insert(2, 'beta');
DEALLOCATE prep_insert;

-- DEALLOCATE ALL variant
PREPARE prep_count AS SELECT COUNT(*) FROM prepare_tbl;
DEALLOCATE ALL;

DROP TABLE prepare_tbl;
