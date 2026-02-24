--
-- TRUNCATE regression coverage
--

CREATE TABLE truncate_a (id INTEGER, payload TEXT);
CREATE TABLE truncate_b (id INTEGER, payload TEXT);

INSERT INTO truncate_a VALUES (1, 'x'), (2, 'y');
INSERT INTO truncate_b VALUES (10, 'p'), (20, 'q');

-- Basic TRUNCATE TABLE form
TRUNCATE TABLE truncate_a;

INSERT INTO truncate_a VALUES (3, 'z');

-- Multi-table TRUNCATE form
TRUNCATE TABLE truncate_a, truncate_b;

INSERT INTO truncate_a VALUES (4, 'k');

-- CASCADE variant accepted by parser
TRUNCATE TABLE truncate_a CASCADE;

DROP TABLE truncate_a;
DROP TABLE truncate_b;
