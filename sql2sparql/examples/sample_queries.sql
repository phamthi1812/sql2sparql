-- SQL2SPARQL Example Queries
-- These queries demonstrate various SQL constructs that can be converted to SPARQL

-- 1. Simple SELECT query
SELECT name, email FROM client;

-- 2. SELECT with WHERE condition
SELECT name, price FROM product WHERE price > 100;

-- 3. SELECT with multiple WHERE conditions
SELECT name, category, price
FROM product
WHERE category = 'Electronics' AND price < 500;

-- 4. SELECT with pattern matching
SELECT name, email
FROM client
WHERE email LIKE '%example.com';

-- 5. JOIN query - Clients and their orders
SELECT client.name, order.date, order.total
FROM client, order
WHERE client.subject = order.client_id;

-- 6. Multiple JOIN - Order details with client and product info
SELECT client.name, product.name, orderitem.quantity
FROM client, order, orderitem, product
WHERE client.subject = order.client_id
  AND order.subject = orderitem.order
  AND orderitem.product = product.subject;

-- 7. Aggregate functions - Count products by category
SELECT category, COUNT(name)
FROM product
GROUP BY category;

-- 8. Aggregate with HAVING clause
SELECT category, COUNT(name), AVG(price)
FROM product
GROUP BY category
HAVING COUNT(name) > 1;

-- 9. ORDER BY with LIMIT
SELECT name, price
FROM product
ORDER BY price DESC
LIMIT 5;

-- 10. Complex aggregate - Total sales per client
SELECT client.name, SUM(order.total)
FROM client, order
WHERE client.subject = order.client_id
GROUP BY client.name
ORDER BY SUM(order.total) DESC;

-- 11. Find clients with pending orders
SELECT DISTINCT client.name, client.email
FROM client, order
WHERE client.subject = order.client_id
  AND order.status = 'pending';

-- 12. Products with low stock
SELECT name, stock
FROM product
WHERE stock < 20
ORDER BY stock ASC;

-- 13. Recent orders
SELECT client.name, order.date, order.total
FROM client, order
WHERE client.subject = order.client_id
  AND order.date > '2024-01-01'
ORDER BY order.date DESC;

-- 14. INSERT new client
INSERT INTO client (name, email, age, status)
VALUES ('Alice Brown', 'alice.brown@example.com', 30, 'active');

-- 15. INSERT new product
INSERT INTO product (name, category, price, stock)
VALUES ('Mechanical Keyboard', 'Electronics', 149.99, 25);

-- 16. DELETE inactive clients
DELETE FROM client WHERE status = 'inactive';

-- 17. DELETE old orders
DELETE FROM order WHERE date < '2023-01-01';

-- 18. UNION query - All entities with names
SELECT name FROM client
UNION
SELECT name FROM product
UNION
SELECT name FROM supplier;

-- 19. Complex filtering with calculations
SELECT name, price, stock, (price * stock) AS inventory_value
FROM product
WHERE (price * stock) > 1000;

-- 20. Find suppliers by country
SELECT name, contact
FROM supplier
WHERE country = 'USA';