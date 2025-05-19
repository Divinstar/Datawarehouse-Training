-- Assignment two Books assignment
--CRUD operations 

--1.add a new book
insert into book (bookid, title, author, genre, price, publishedyear, stock)
values (6, 'deep work', 'cal newport', 'self-help', 420, 2016, 35);

--2.update the book price
update book
set price = price + 50
where genre = 'self-help';

--3. delete a book
delete from book
where bookid = 4;

--4. read all books
select * from book
order by title asc;

-- SORTING AND FILTERING
--5.sort by price
select * from book
order by price desc;

--6.filter by genre
select * from book
where genre = 'fiction';

--7.	filter with AND condition
select * from book
where genre = 'self-help' and price > 400;


--8.filter with OR
select * from book
where genre = 'fiction' or publishedyear > 2000;

--AGGREGATION and GROUPing
--9.total stock value
	select sum(price * stock) as totalstockvalue
from book;

--10. avg price by genre
select genre, avg(price) as averageprice
from book
group by genre;

--11.count books by paulo cohello
select count(*) as totalbooks
from book
where author = 'paulo coelho';

--12.find a book by keyword
select * from book
where title like '%the%';

--13.filter by multiple conditions
select * from book
where author = 'yuval noah harari' and price < 600;

--14.book priced between 300 and 500
select * from book
where price between 300 and 500;

-- ADVANCED QUERIES
--15.top 3 expensive queries
select top 3 * from book
order by price desc;

--16.bppks ubished before a specific year
select * from book
where publishedyear < 2000;

--17.group by genre
select genre, count(*) as bookcount
from book
group by genre;


--18. find the duplicate title
select title, count(*) as count
from book
group by title
having count(*) > 1;

--JOIN AND SUBqueries
--19.author with most books
from book
group by author
order by bookcount desc;

--20.oldest book by genre
select genre, min(publishedyear) as oldestyear
from book
group by genre;


