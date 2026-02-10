CREATE TABLE accounts (id int8 PRIMARY KEY, email text);
INSERT INTO accounts VALUES (1, 'a@example.com'), (2, 'b@example.com');
-- expect: 2
SELECT count(*) FROM accounts;
-- expect: 1|a@example.com
-- expect: 2|b@example.com
SELECT id, email FROM accounts ORDER BY id;
