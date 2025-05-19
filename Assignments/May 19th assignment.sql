create table employee_attendance (
    attendanceid int primary key,
    employeename varchar(100),
    department varchar(50),
    date date,
    status varchar(20),
    hoursworked int
);

insert into employee_attendance values
(1, 'John Doe', 'IT', '2025-05-01', 'Present', 8),
(2, 'Priya Singh', 'HR', '2025-05-01', 'Absent', 0),
(3, 'Ali Khan', 'IT', '2025-05-01', 'Present', 7),
(4, 'Riya Patel', 'Sales', '2025-05-01', 'Late', 6),
(5, 'David Brown', 'Marketing', '2025-05-01', 'Present', 8);

--CRUD OPERATIONS
--1.add new attentandance rec
insert into employee_attendance values
(6, 'Neha Sharma', 'Finance', '2025-05-01', 'Present', 8);

--2.update attendacne status
update employee_attendance
set status = 'Present'
where employeename = 'Riya Patel' and date = '2025-05-01';

--3.delet
delete from employee_attendance
where employeename = 'Priya Singh' and date = '2025-05-01';

--4.read records
select * from employee_attendance
order by employeename asc;

--SORTING AND FILTERING
--5.sort by hours worked
select * from employee_attendance
order by hoursworked desc;

--6.filter by dept
select * from employee_attendance
where department = 'IT';

--7.filter with AND
select * from employee_attendance
where department = 'IT' and status = 'Present';

--8. filter with OR
select * from employee_attendance
where status = 'Absent' or status = 'Late';


--AGGREGATION AND GROUPING
--9. tot hours worked by dept
select department, sum(hoursworked) as total_hours
from employee_attendance
group by department;

--10. avg hours worked
select avg(hoursworked) as avg_hours
from employee_attendance;

--11. attendacne count by status
select status, count(*) as count
from employee_attendance
group by status;


--CONDITIONAL AND PATTERN
--12.find emp by name
select * from employee_attendance
where employeename like 'R%';

--13.filter by mul conditions
select * from employee_attendance
where hoursworked > 6 and status = 'Present';

--14.filter using BETWEEN
select * from employee_attendance
where hoursworked between 6 and 8;

--ADVANCED QUERIES
--15. top 2 employess with most hrs
select top 2 * from employee_attendance
order by hoursworked desc;

--16. emp with less than avg hrs
select * from employee_attendance
where hoursworked < (select avg(hoursworked) from employee_attendance);

--17.group by statis
select status, avg(hoursworked) as avg_hours
from employee_attendance
group by status;


--18. find duplicate entries
select employeename, date, count(*) as entry_count
from employee_attendance
group by employeename, date
having count(*) > 1;


--JOIN AND SUBQUERIES
--19. department and present emp
select top 1 department, count(*) as present_count
from employee_attendance
where status = 'Present'
group by department
order by count(*) desc;

--20.emp with max hrs per department
select ea.department, ea.employeename, ea.hoursworked
from employee_attendance ea
join (
    select department, max(hoursworked) as max_hours
    from employee_attendance
    group by department
) temp on ea.department = temp.department and ea.hoursworked = temp.max_hours;
