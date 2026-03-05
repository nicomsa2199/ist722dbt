with orders as (

    select *
    from {{ source('northwind','ORDERS') }}

),

order_details as (

    select *
    from {{ source('northwind','ORDER_DETAILS') }}

),

dim_customer as (

    select *
    from {{ ref('dim_customer') }}

),

dim_employee as (

    select *
    from {{ ref('dim_employee') }}

),

dim_product as (

    select *
    from {{ ref('dim_product') }}

),

dim_date as (

    select *
    from {{ ref('dim_date') }}

)

select

    o.ORDERID as orderid,

    c.customerkey,
    e.employeekey,
    d.datekey as orderdatekey,
    p.productkey,

    od.QUANTITY as quantity,

    od.QUANTITY * od.UNITPRICE as extendedpriceamount,

    (od.QUANTITY * od.UNITPRICE) * od.DISCOUNT as discountamount,

    (od.QUANTITY * od.UNITPRICE) - ((od.QUANTITY * od.UNITPRICE) * od.DISCOUNT) as soldamount

from orders o

join order_details od
    on o.ORDERID = od.ORDERID

left join dim_customer c
    on o.CUSTOMERID = c.customerid

left join dim_employee e
    on o.EMPLOYEEID = e.employeeid

left join dim_product p
    on od.PRODUCTID = p.productid

left join dim_date d
    on o.ORDERDATE = d.date
    