# Postrust CLI Demo
Output assets/demo.gif

Set Shell "bash"
Set FontSize 14
Set Width 1200
Set Height 600
Set Theme "Dracula"
Set Padding 20
Set TypingSpeed 40ms

# Start server and set PATH silently
Hide
Type "export PATH=/home/linuxbrew/.linuxbrew/opt/libpq/bin:/home/linuxbrew/.linuxbrew/bin:$PATH"
Enter
Sleep 500ms
Type "pkill -f pg_server 2>/dev/null; sleep 2; /home/rosssaunders/code/rosssaunders/postrust/target/release/pg_server >/dev/null 2>&1 &"
Enter
Sleep 4s
Type "clear"
Enter
Sleep 500ms
Show

# Connect to Postrust
Type "psql -h 127.0.0.1 -p 55432 -U postgres"
Enter
Sleep 2s

# Create a table
Type "CREATE TABLE orders (item TEXT, qty INT, price NUMERIC(10,2));"
Enter
Sleep 1s

# Insert data
Type "INSERT INTO orders VALUES ('Widget', 100, 9.99), ('Gadget', 50, 24.95), ('Doohickey', 200, 3.50);"
Enter
Sleep 1s

# Query with aggregation
Type "SELECT item, qty * price AS total FROM orders ORDER BY total DESC;"
Enter
Sleep 2s

# Window function
Type "SELECT item, price, RANK() OVER (ORDER BY price DESC) FROM orders;"
Enter
Sleep 2s

# JSON output
Type "SELECT jsonb_build_object('item', item, 'total', qty * price) AS doc FROM orders;"
Enter
Sleep 2s

# CTE
Type "WITH summary AS (SELECT COUNT(*) AS items, SUM(qty * price) AS revenue FROM orders) SELECT * FROM summary;"
Enter
Sleep 2s

# HTTP extension - live API call
Type "CREATE EXTENSION http;"
Enter
Sleep 1s

Type "SELECT content::jsonb->'origin' AS my_ip FROM http_get('https://httpbin.org/ip');"
Enter
Sleep 4s

# Exit
Type "\q"
Enter
Sleep 500ms
