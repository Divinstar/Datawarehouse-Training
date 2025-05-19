create table productinventory (
    productid int primary key,
    productname varchar(100),
    category varchar(50),
    quantity int,
    unitprice int,
    supplier varchar(100),
    lastrestocked date
);

insert into productinventory values
(1, 'Laptop', 'Electronics', 20, 70000, 'TechMart', '2025-04-20'),
(2, 'Office Chair', 'Furniture', 50, 5000, 'HomeComfort', '2025-04-18'),
(3, 'Smartwatch', 'Electronics', 30, 15000, 'GadgetHub', '2025-04-22'),
(4, 'Desk Lamp', 'Lighting', 80, 1200, 'BrightLife', '2025-04-25'),
(5, 'Wireless Mouse', 'Electronics', 100, 1500, 'GadgetHub', '2025-04-30');



--CRUD OPERATIONS
--1.add
insert into productinventory values
(6, 'Gaming Keyboard', 'Electronics', 40, 3500, 'TechMart', '2025-05-01');

--2.update
update productinventory
set quantity = quantity + 20
where productname = 'Desk Lamp';

--3.delete
delete from productinventory
where productid = 2;

--4. read all
select * from productinventory
order by productname asc;

--SORTINE AND FILTERING
--5.Sort by Quantity descending
select * from productinventory
order by quantity desc;

--6.Display all Electronics products
select * from productinventory
where category = 'Electronics';

--7.Electronics products with Quantity > 20
select * from productinventory
where category = 'Electronics' and quantity > 20;

--8.Electronics OR UnitPrice < 2000
select * from productinventory
where category = 'Electronics' or unitprice < 2000;

--9.Total stock value (Quantity * UnitPrice)
select sum(quantity * unitprice) as total_stock_value
from productinventory;


--10.Average price grouped by Category
select category, avg(unitprice) as avg_price
from productinventory
group by category;

--11.Count of products from GadgetHub
select count(*) as product_count
from productinventory
where supplier = 'GadgetHub';

--12. Products starting with 'W'
select * from productinventory
where productname like 'W%';

--13. GadgetHub products with UnitPrice > 10000
select * from productinventory
where supplier = 'GadgetHub' and unitprice > 10000;

--14.UnitPrice between 1000 and 20000
select * from productinventory
where unitprice between 1000 and 20000;

--15.Top 3 most expensive products
select top 3 * from productinventory
order by unitprice desc;

--16. Products restocked in last 10 days
select * from productinventory
where lastrestocked >= dateadd(day, -10, getdate());

--17.	Total quantity from each Supplier
select supplier, sum(quantity) as total_quantity
from productinventory
group by supplier;

--18.Products with Quantity < 30
select * from productinventory
where quantity < 30;


--19.Supplier with most products
select top 1 supplier, count(*) as product_count
from productinventory
group by supplier
order by count(*) desc;

--20.Product with highest stock value (Quantity * UnitPrice)
select top 1 *, (quantity * unitprice) as stock_value
from productinventory
order by stock_value desc;





