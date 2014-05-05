--1.b
SELECT relname, relkind, reltuples, relpages
FROM pg_class
WHERE relname LIKE 'categories%';

SELECT relname, relkind, reltuples, relpages
FROM pg_class
WHERE relname LIKE 'cust_hist%';

SELECT relname, relkind, reltuples, relpages
FROM pg_class
WHERE relname LIKE 'customers%';

SELECT relname, relkind, reltuples, relpages
FROM pg_class
WHERE relname LIKE 'inventory%';

SELECT relname, relkind, reltuples, relpages
FROM pg_class
WHERE relname LIKE 'orderlines%';

SELECT relname, relkind, reltuples, relpages
FROM pg_class
WHERE relname LIKE 'orders%';

SELECT relname, relkind, reltuples, relpages
FROM pg_class
WHERE relname LIKE 'products%';

SELECT relname, relkind, reltuples, relpages
FROM pg_class
WHERE relname LIKE 'reorder%';

--1.c
SELECT attname, n_distinct
FROM pg_stats
WHERE tablename = 'customers';

--1.d
SELECT count(*)
FROM (SELECT DISTINCT customerid FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT firstname FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT lastname FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT address1 FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT address2 FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT city FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT state FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT zip FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT country FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT region FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT email FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT phone FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT creditcardtype FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT creditcard FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT creditcardexpiration FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT username FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT password FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT age FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT income FROM Customers) t;

SELECT count(*)
FROM (SELECT DISTINCT gender FROM Customers) t;
--2.a
EXPLAIN select * from customers where country = 'Japan';
select * from customers where country = 'Japan';

--3.a
\i setup_db.sql;
VACUUM;
ANALYZE;

CREATE INDEX customers_country ON customers(country);
VACUUM;
ANALYZE;

SELECT relname, relkind, reltuples, relpages
FROM pg_class
WHERE relname LIKE 'customers%';

--3.b
EXPLAIN select * from customers where country = 'Japan';

--3.d
CLUSTER customers_country ON customers;
VACUUM;
ANALYZE;

EXPLAIN select * from customers where country = 'Japan';

--4.a
\i setup_db.sql;
VACUUM;
ANALYZE;

EXPLAIN SELECT totalamount
FROM Customers C, Orders O
WHERE C.customerid = O.customerid AND C.country = 'Japan';

--4.c
SET enable_hashjoin = off;

EXPLAIN SELECT totalamount
FROM Customers C, Orders O
WHERE C.customerid = O.customerid AND C.country = 'Japan';

--4.d
SET enable_mergejoin = off;

EXPLAIN SELECT totalamount
FROM Customers C, Orders O
WHERE C.customerid = O.customerid AND C.country = 'Japan';

--5.a
\i setup_db.sql;
VACUUM;
ANALYZE;

SET enable_hashjoin = on;
SET enable_mergejoin = on;

EXPLAIN SELECT AVG(totalamount) as avgOrder, country
FROM Customers C, Orders O
WHERE C.customerid = O.customerid
GROUP BY country
ORDER BY avgOrder;

SET enable_hashjoin = off;

EXPLAIN SELECT AVG(totalamount) as avgOrder, country
FROM Customers C, Orders O
WHERE C.customerid = O.customerid
GROUP BY country
ORDER BY avgOrder;

--5.b
SET enable_hashjoin = on;

EXPLAIN SELECT *
FROM Customers C, Orders O
WHERE C.customerid = O.customerid
ORDER BY C.customerid;

SET enable_mergejoin = off;

EXPLAIN SELECT *
FROM Customers C, Orders O
WHERE C.customerid = O.customerid
ORDER BY C.customerid;

--6.a
\i setup_db.sql;
VACUUM;
ANALYZE;

Explain SELECT C.customerid, C.lastname
FROM Customers C
WHERE 
4 < ( SELECT COUNT(*)
FROM Orders O
WHERE O.customerid = C.customerid);

--6.b
CREATE VIEW OrderCount(customerid, numorders) AS
SELECT C.customerid, count(*)
FROM Customers C, Orders O
WHERE O.customerid = C.customerid
GROUP BY C.customerid;

--6.c
SELECT C.customerid, C.lastname
FROM Customers C, OrderCount OC
WHERE
C.customerid = OC.customerid AND 4 < OC.numorders;

--6.d
EXPLAIN SELECT C.customerid, C.lastname
FROM Customers C, OrderCount OC
WHERE
C.customerid = OC.customerid AND 4 < OC.numorders;

--7.a 
\i setup_db.sql;
VACUUM;
ANALYZE;

EXPLAIN SELECT customerid, lastname, numorders
FROM (
SELECT C.customerid, C.lastname, count(*) as numorders
		FROM Customers C, Orders O
		WHERE C.customerid = O.customerid AND C.country = 'Japan'
		GROUP BY C.customerid, lastname) AS ORDERCOUNTS1
	WHERE 5 >= (SELECT count(*)
           FROM (
		SELECT C.customerid, C.lastname, count(*) as numorders
		FROM Customers C, Orders O
		WHERE C.customerid=O.customerid AND C.country = 'Japan'
		GROUP BY C.customerid, lastname) AS ORDERCOUNTS2
WHERE ORDERCOUNTS1.numorders < ORDERCOUNTS2.numorders)
ORDER BY customerid;

--7.b
CREATE VIEW OrderCountJapan(customerid, numorders) AS
SELECT C.customerid, count(*)
FROM Customers C, Orders O
WHERE O.customerid = C.customerid AND C.country = 'Japan'
GROUP BY C.customerid;

CREATE VIEW MoreFrequentJapanCustomers(customerid, oRank) AS
SELECT OCJ1.customerid, count(*)
FROM OrderCountJapan OCJ1 LEFT JOIN OrderCountJapan OCJ2
ON OCJ1.numorders < OCJ2.numorders
GROUP BY OCJ1.customerid;

--7.c
SELECT C.customerid, C.lastname, MFJC.oRank, OCJ.numorders
FROM MoreFrequentJapanCustomers MFJC, Customers C, OrderCountJapan OCJ
WHERE MFJC.customerid = C.customerid AND MFJC.oRank <=5 AND C.customerid = OCJ.customerid
ORDER BY C.customerid;

--7.d
EXPLAIN SELECT C.customerid, C.lastname, MFJC.oRank, OCJ.numorders
FROM MoreFrequentJapanCustomers MFJC, Customers C, OrderCountJapan OCJ
WHERE MFJC.customerid = C.customerid AND MFJC.oRank <=5 AND C.customerid = OCJ.customerid
ORDER BY C.customerid;

