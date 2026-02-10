CREATE TABLE catalog_items (id int8);
-- expect: catalog_items
SELECT relname FROM pg_class WHERE relname = 'catalog_items';
-- expect: catalog_items
SELECT table_name FROM information_schema.tables WHERE table_name = 'catalog_items';
