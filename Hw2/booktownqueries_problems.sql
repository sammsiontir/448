-- Homework #2: EECS 484.
-- Your uniquname or UMID: chengu

-- Uncomment the following line if using sqlite3 and running in batch mode.
-- .echo ON

-- Q0: "list titles of all books". Answer given below.

SELECT title FROM books;

-- Q1: list author last name and titles of all books with the subject "Horror".

SELECT A.last_name, B.title 
FROM authors A, books B, subjects S 
WHERE A.author_id = B.author_id 
AND   S.subject_id = B.subject_id
AND   S.subject = 'Horror';

-- Q2: list last name and first name of authors who have written both
-- Short Story and Horror books. This may require nested queries.
-- Be careful here. In general, there could be two different authors
-- with the same name, one who has written a horror book and another
-- who has written short stories. 

SELECT A.last_name, A.first_name
FROM authors A
WHERE A.author_id IN (SELECT B.author_id
                      FROM authors A, books B, subjects S 
                      WHERE A.author_id = B.author_id 
                      AND   S.subject_id = B.subject_id
                      AND   S.subject = 'Short Story'
                      INTERSECT
                      SELECT B.author_id
                      FROM authors A, books B, subjects S 
                      WHERE A.author_id = B.author_id 
                      AND   S.subject_id = B.subject_id
                      AND   S.subject = 'Horror');

-- Q3: list titles, author's last name, and author's first name of all books 
-- by authors who have written Horror book(s). Note: that this may
-- require a nested query. The answer can include non-Horror books. You
-- can also use views. But DROP any views at the end of your query. Using
-- a single query is likely to be more efficient in practice.

SELECT B.title, A.last_name, A.first_name
FROM authors A, books B
WHERE A.author_id = B.author_id 
AND   B.author_id IN (SELECT A.author_id
                      FROM authors A, books B, subjects S 
                      WHERE A.author_id = B.author_id 
                      AND   S.subject_id = B.subject_id
                      AND   S.subject = 'Horror');


-- Q4: Find the id of authors who have written exactly
--   1 book. Name the column as 'id'.

SELECT A.author_id 
AS id
FROM authors A, books B
WHERE A.author_id = B.author_id 
GROUP BY A.author_id
HAVING count(*)=1;

-- Q5: Find the title of all books for subjects that
--   have 3 or more books.
SELECT B.title
FROM books B
WHERE B.subject_id IN (SELECT B.subject_id
                       FROM books B, subjects S 
                       WHERE B.subject_id = S.subject_id
                       GROUP BY S.subject_id
                       HAVING count(*)>=3);

