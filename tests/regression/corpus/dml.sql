CREATE TABLE dml_accounts (id int8 PRIMARY KEY, name text, balance int8);
INSERT INTO dml_accounts VALUES (1, 'a', 10), (2, 'b', 20);
-- expect: 2
SELECT count(*) FROM dml_accounts;
-- expect: 3
INSERT INTO dml_accounts VALUES (3, 'c', 30) RETURNING id;
-- expect: 15
UPDATE dml_accounts SET balance = balance + 5 WHERE id = 1 RETURNING balance;
-- expect: 2
DELETE FROM dml_accounts WHERE id = 2 RETURNING id;
CREATE TABLE dml_source (id int8, name text, balance int8);
INSERT INTO dml_source VALUES (1, 'a2', 11), (4, 'd', 40);
MERGE INTO dml_accounts AS t
USING dml_source AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET name = s.name, balance = s.balance
WHEN NOT MATCHED THEN INSERT (id, name, balance) VALUES (s.id, s.name, s.balance);
-- expect: 1|a2|11
-- expect: 3|c|30
-- expect: 4|d|40
SELECT id, name, balance FROM dml_accounts ORDER BY id;
