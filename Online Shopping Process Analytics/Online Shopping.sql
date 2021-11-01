create database miniproject;
use miniproject;

#1. Join all the tables and create a new table called combined_table.
#(market_fact, cust_dimen, orders_dimen, prod_dimen, shipping_dimen)

create table combined_table
(select c.customer_name,c.cust_id,o.order_id,m.ord_id,m.prod_id,m.ship_id,m.sales,m.discount,m.order_quantity,m.profit,m.shipping_cost,m.product_base_margin,
o.order_date,s.ship_date,s.ship_mode,o.order_priority,c.province,c.region,c.customer_segment,p.product_category,p.product_Sub_Category 
from market_fact m 
join cust_dimen c on m.cust_id=c.cust_id
join orders_dimen o on m.ord_id = o.ord_id
join shipping_dimen s on m.ship_id=s.ship_id
join prod_dimen p on m.prod_id=p.prod_id);

select*from combined_table;

#2. Find the top 3 customers who have the maximum number of orders

select*from
(select customer_name,count(ord_id) over(partition by customer_name) as max_order from combined_table)t
group by customer_name order by max_order desc limit 3;

#3. Create a new column DaysTakenForDelivery that contains the 
# date difference of Order_Date and Ship_Date.

alter table combined_table add column DaysTakenForDelivery int;

update combined_table set DaysTakenForDelivery=datediff(str_to_date(ship_date,"%d-%m-%Y"),str_to_date(order_date,"%d-%m-%Y"));

select*from combined_table;

#4. Find the customer whose order took the maximum time to get delivered.

select customer_name,max(daystakenfordelivery) from combined_table;

#5. Retrieve total sales made by each product from the data (use Windows function)

select*from combined_table;
select distinct product_sub_category,sum(sales)over(partition by product_sub_category) 
as tot_sales_products from combined_table where product_sub_category is not null ;

#6. Retrieve total profit made from each product from the data (use windows function)

select distinct product_sub_category,sum(profit)over(partition by product_sub_category) 
as tot_sales_products from combined_table where product_sub_category is not null ; # inference: 4 products have loss

#7. Count the total number of unique customers in January and how many of them came back every month over the entire year in 2011.

#i) unique customers in January
select count(unique_customers),Month from
(select distinct customer_name as unique_customers,month(str_to_date(order_date,"%d-%m-%Y")) as Month from combined_table 
where month(str_to_date(order_date,"%d-%m-%Y")) = 1)t;

#ii) unique customers in January and how many of them came back every month over the entire year in 2011

select count(unique_customers)from
(select distinct customer_name as unique_customers,month(str_to_date(order_date,"%d-%m-%Y")) as Month 
from combined_table where month(str_to_date(order_date,"%d-%m-%Y")) = 1 and 
month(str_to_date(order_date,"%d-%m-%Y"))=all(select month(str_to_date(order_date,"%d-%m-%Y")) 
from combined_table where year(str_to_date(order_date,"%d-%m-%Y"))=2011) and year(str_to_date(order_date,"%d-%m-%Y"))=2011)t;

#inference : there are no customers who came back after every month over the year 2011

#8. Retrieve month-by-month customer retention rate since the start of the business.(using views)
#Tips:
#1: Create a view where each user’s visits are logged by month, allowing   the possibility that these will have occurred over multiple # years since whenever business started operations
# 2: Identify the time lapse between each visit. So, for each person and for each month, we see when the next visit is.
# 3: Calculate the time gaps between visits
# 4: categorise the customer with time gap 1 as retained, >1 as irregular and NULL as churned
# 5: calculate the retention month wise

create or replace view  total_customers as
select distinct month(str_to_date(order_date,"%d-%m-%Y")) m1,count(*) over(partition by month(str_to_date(order_date,"%d-%m-%Y"))) 
as sum from combined_table
where month(str_to_date(order_date,"%d-%m-%Y")) is not null;

select*from total_customers;

create or replace view retention_stats
as
select distinct month as m1,count(customer_name) over(partition by month) as retention,status from(
select * ,case when gap=1 then "Retained" when gap >1 then "Irregular" else "Churned" end as "status"from(
select *,abs(month(next_visit)-month(visit)) as gap from
(select customer_name,str_to_date(order_date,"%d-%m-%Y") visit,lead(str_to_date(order_date,"%d-%m-%Y")) over (partition by customer_name order by str_to_date(order_date,"%d-%m-%Y")) next_visit ,
month(str_to_date(order_date,"%d-%m-%Y")) month from combined_table where customer_name is not null order by customer_name)t)t2
where gap<>0 or gap is null)t3 where status="retained";

select*from retention_stats;

#creating a view retention_rate by joining the above two views

create or replace view retention_rate as
select t.m1 as months,r.retention/t.sum*100 as `retention rate %` from total_customers t
join retention_stats r on t.m1=r.m1;

select*from retention_rate



