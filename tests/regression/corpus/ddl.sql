CREATE SCHEMA ddl_schema;
CREATE SEQUENCE ddl_schema.item_seq START 10;
CREATE TABLE ddl_schema.items (
    id int8 PRIMARY KEY,
    name text,
    created_at date DEFAULT make_date(2024, 1, 1)
);
INSERT INTO ddl_schema.items (id, name) VALUES (1, 'alpha'), (2, 'beta');
ALTER TABLE ddl_schema.items ADD COLUMN active boolean DEFAULT true;
ALTER TABLE ddl_schema.items RENAME COLUMN name TO label;
CREATE INDEX items_name_idx ON ddl_schema.items (label);
CREATE VIEW ddl_schema.items_view AS SELECT id, label FROM ddl_schema.items;
-- expect: 10
SELECT nextval('ddl_schema.item_seq');
-- expect: 1|alpha
-- expect: 2|beta
SELECT id, label FROM ddl_schema.items_view ORDER BY id;
DROP VIEW ddl_schema.items_view;
DROP INDEX ddl_schema.items_name_idx;
DROP TABLE ddl_schema.items;
DROP SEQUENCE ddl_schema.item_seq;
DROP SCHEMA ddl_schema;
