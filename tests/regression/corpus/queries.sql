CREATE TABLE q_customers (customer_id int8, customer_name text, region text);
CREATE TABLE q_orders (order_id int8, customer_ref int8, amount int8);
INSERT INTO q_customers VALUES (1, 'alice', 'north'), (2, 'bob', 'south');
INSERT INTO q_orders VALUES (1, 1, 100), (2, 1, 50), (3, 2, 200);
-- expect: alice|north|150
-- expect: bob|south|200
SELECT customer_name, region, sum(amount)
FROM q_customers
JOIN q_orders ON customer_id = customer_ref
GROUP BY customer_name, region
ORDER BY customer_name;
-- expect: bob
SELECT customer_name
FROM q_customers
WHERE customer_id IN (
    SELECT customer_ref
    FROM q_orders
    WHERE amount > (SELECT avg(amount) FROM q_orders)
)
ORDER BY customer_name;
WITH ranked AS (
    SELECT customer_name,
           amount,
           row_number() OVER (
               PARTITION BY customer_name
               ORDER BY amount DESC
           ) AS rn
    FROM q_customers
    JOIN q_orders ON customer_id = customer_ref
)
-- expect: alice|100
-- expect: bob|200
SELECT customer_name, amount
FROM ranked
WHERE rn = 1
ORDER BY customer_name;
-- expect: alice|north|150
-- expect: bob|south|200
SELECT customer_name, region, sum(amount)
FROM q_customers
JOIN q_orders ON customer_id = customer_ref
GROUP BY GROUPING SETS ((customer_name, region), (region))
ORDER BY region, customer_name;
