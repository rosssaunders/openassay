CREATE TABLE txn_items (id int8, note text);
BEGIN;
INSERT INTO txn_items VALUES (1, 'a');
ROLLBACK;
-- expect: 0
SELECT count(*) FROM txn_items;
BEGIN;
INSERT INTO txn_items VALUES (2, 'b');
SAVEPOINT s1;
INSERT INTO txn_items VALUES (3, 'c');
ROLLBACK TO s1;
COMMIT;
-- expect: 1
SELECT count(*) FROM txn_items;
