-- Northwind SQL Test Queries for SQL2SPARQL Conversion
-- These queries test various SQL features with the Northwind dataset

-- 1. Simple SELECT - Get all customers
SELECT customerID, companyName, contactName, city, country 
FROM customer;

-- 2. SELECT with WHERE - Get customers from specific country
SELECT customerID, companyName, city 
FROM customer 
WHERE country = 'Germany';

-- 3. SELECT with multiple conditions - Get expensive products
SELECT productName, unitPrice, unitsInStock 
FROM product 
WHERE unitPrice > 20 AND unitsInStock < 50;

-- 4. SELECT with LIKE - Find products with specific name pattern
SELECT productName, categoryID 
FROM product 
WHERE productName LIKE 'Chef%';

-- 5. Simple JOIN - Customers and their orders
SELECT c.companyName, c.contactName, o.orderID, o.orderDate 
FROM customer c, orders o 
WHERE c.customerID = o.customerID;

-- 6. Complex JOIN - Order details with product and customer info
SELECT c.companyName, p.productName, od.quantity, od.unitPrice 
FROM customer c, orders o, orderdetail od, product p 
WHERE c.customerID = o.customerID 
  AND o.orderID = od.orderID 
  AND od.productID = p.productID;

-- 7. JOIN with filtering - Recent orders for specific customer
SELECT c.companyName, o.orderID, o.orderDate, o.freight 
FROM customer c, orders o 
WHERE c.customerID = o.customerID 
  AND c.companyName = 'Alfreds Futterkiste' 
  AND o.orderDate > '1996-07-01';

-- 8. Aggregate - Count products by category
SELECT cat.categoryName, COUNT(p.productID) as product_count 
FROM product p, category cat 
WHERE p.categoryID = cat.categoryID 
GROUP BY cat.categoryName;

-- 9. Aggregate with HAVING - Categories with multiple products
SELECT cat.categoryName, COUNT(p.productID) as product_count, AVG(p.unitPrice) as avg_price 
FROM product p, category cat 
WHERE p.categoryID = cat.categoryID 
GROUP BY cat.categoryName 
HAVING COUNT(p.productID) > 1;

-- 10. Complex aggregate - Total sales per customer
SELECT c.companyName, COUNT(DISTINCT o.orderID) as order_count, SUM(od.quantity * od.unitPrice) as total_sales 
FROM customer c, orders o, orderdetail od 
WHERE c.customerID = o.customerID 
  AND o.orderID = od.orderID 
GROUP BY c.companyName 
ORDER BY total_sales DESC;

-- 11. Subquery - Products above average price
SELECT productName, unitPrice 
FROM product 
WHERE unitPrice > (SELECT AVG(unitPrice) FROM product);

-- 12. JOIN with date range - Orders in specific date range
SELECT c.companyName, o.orderID, o.orderDate, o.shipCountry 
FROM customer c, orders o 
WHERE c.customerID = o.customerID 
  AND o.orderDate BETWEEN '1996-07-01' AND '1996-07-31';

-- 13. Multiple table JOIN - Complete order information
SELECT c.companyName, e.firstName || ' ' || e.lastName as employee_name, 
       o.orderDate, p.productName, od.quantity, od.unitPrice,
       (od.quantity * od.unitPrice) as line_total
FROM customer c, orders o, orderdetail od, product p, employee e
WHERE c.customerID = o.customerID 
  AND o.orderID = od.orderID 
  AND od.productID = p.productID 
  AND o.employeeID = e.employeeID;

-- 14. Aggregate with JOIN - Products per supplier
SELECT s.companyName, COUNT(p.productID) as product_count 
FROM supplier s, product p 
WHERE s.supplierID = p.supplierID 
GROUP BY s.companyName 
HAVING COUNT(p.productID) >= 2;

-- 15. Complex filtering - Discontinued products with stock
SELECT productName, unitPrice, unitsInStock, unitsOnOrder 
FROM product 
WHERE discontinued = true AND (unitsInStock > 0 OR unitsOnOrder > 0);

-- 16. ORDER BY with LIMIT - Top 5 most expensive products
SELECT productName, unitPrice, categoryName 
FROM product p, category cat 
WHERE p.categoryID = cat.categoryID 
ORDER BY unitPrice DESC 
LIMIT 5;

-- 17. COUNT with GROUP BY - Orders per employee
SELECT e.firstName || ' ' || e.lastName as employee_name, 
       COUNT(o.orderID) as order_count 
FROM employee e, orders o 
WHERE e.employeeID = o.employeeID 
GROUP BY e.employeeID, e.firstName, e.lastName 
ORDER BY order_count DESC;

-- 18. Date functions - Orders by month
SELECT strftime('%Y-%m', orderDate) as order_month, 
       COUNT(orderID) as order_count,
       SUM(freight) as total_freight 
FROM orders 
GROUP BY strftime('%Y-%m', orderDate) 
ORDER BY order_month;

-- 19. Conditional aggregation - Products by stock status
SELECT categoryName,
       COUNT(CASE WHEN unitsInStock > 20 THEN 1 END) as well_stocked,
       COUNT(CASE WHEN unitsInStock BETWEEN 1 AND 20 THEN 1 END) as low_stock,
       COUNT(CASE WHEN unitsInStock = 0 THEN 1 END) as out_of_stock
FROM product p, category cat 
WHERE p.categoryID = cat.categoryID 
GROUP BY categoryName;

-- 20. Complex calculation - Inventory value by category
SELECT cat.categoryName, 
       COUNT(p.productID) as product_count,
       SUM(p.unitPrice * p.unitsInStock) as inventory_value,
       AVG(p.unitPrice) as avg_price
FROM product p, category cat 
WHERE p.categoryID = cat.categoryID 
GROUP BY cat.categoryName 
ORDER BY inventory_value DESC;