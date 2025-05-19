CREATE TABLE Product (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Price INT,
    StockQuantity INT,
    Supplier VARCHAR(100)
);

INSERT INTO Product (ProductID, ProductName, Category, Price, StockQuantity, Supplier)
VALUES
(1, 'Laptop', 'Electronics', 70000, 50, 'TechMart'),
(2, 'Office Chair', 'Furniture', 5000, 100, 'HomeComfort'),
(3, 'Smartwatch', 'Electronics', 15000, 200, 'GadgetHub'),
(4, 'Desk Lamp', 'Lighting', 1200, 300, 'BrightLife'),
(5, 'Wireless Mouse', 'Electronics', 1500, 250, 'GadgetHub');



--CRUD OPERATIONS
--1.add product
insert into product (productname, category, price, stockquantity, supplier)
values ('gaming keyboard', 'electronics', 3500, 150, 'techmart');

--2.update price
update product
set price = price * 1.10
where category = 'electronics';

--3.delete a product
delete from product
where productid = 4;

--4. read all
select * from product
order by price desc;


--SORTING AND FILTERING
--5. sort by stcok
select * from product
order by stockquantity asc;

--6. filter by category
select * from product
where category = 'electronics';

--7. filter with AND condition
select * from product
where category = 'electronics' and price > 5000;

--8. filter with OR condition
select * from product
where category = 'electronics' or price < 2000;

--AGGREGATION AND GROUPING
--9. calculate total stock value
select sum(price * stockquantity) as totalstockvalue
from product;

--10. avg price of each category
select category, avg(price) as averageprice
from product
group by category;

--11. total products by supplie(gadethub)
select count(*) as productcount
from product
where supplier = 'gadgethub';

--CONDITIONAL AND PATTERN MATCHING
--12. find products with specific keywords
select * from product
where productname like '%wireless%';

--13. products with multiple suppliers
select * from product
where supplier in ('techmart', 'gadgethub');

--14. filter using BETWEEN operator
select * from product
where price between 1000 and 20000;


--ADVANCED QUERIES
--15. products with high stock
select * from product
where stockquantity > (select avg(stockquantity) from product);

--16. top 3 expensive prodcuts
select top 3 * from product
order by price desc;

--17. duplicate supplier names
select supplier, count(*) as occurrences
from product
group by supplier
having count(*) > 1;

--18. produict summary(count&tot stock val)
select category,
       count(*) as productcount,
       sum(price * stockquantity) as totalstockvalue
from product
group by category;

--JOIN AND SUBQUERIES
--19. supplier with the most products
select top 1 supplier, count(*) as productcount
from product
group by supplier
order by productcount desc;

--20. MOSt expensive products per category
select *
from product p
where price = (
    select max(price)
    from product
    where category = p.category
);


