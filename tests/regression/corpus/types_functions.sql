-- expect: hello world
SELECT concat('hello', ' ', 'world');
-- expect: 4
SELECT abs(-4);
-- expect: 3
SELECT sqrt(9);
-- expect: 2024-01-02
SELECT to_char(make_date(2024, 1, 2), 'YYYY-MM-DD');
-- expect: 3
SELECT jsonb_array_length(jsonb_build_array(1, 2, 3));
-- expect: 1
SELECT jsonb_build_object('a', 1)->>'a';
