with stg_orders as (
  select
    ORDERID,
    {{ dbt_utils.generate_surrogate_key(['EMPLOYEEID']) }} as employeekey,
    {{ dbt_utils.generate_surrogate_key(['CUSTOMERID']) }} as customerkey,
    replace(to_date(ORDERDATE)::varchar,'-','')::int as orderdatekey,
    replace(to_date(SHIPPEDDATE)::varchar,'-','')::int as shippeddatekey,
    replace(to_date(REQUIREDDATE)::varchar,'-','')::int as requireddatekey,
    SHIPNAME,
    SHIPADDRESS,
    SHIPCITY,
    SHIPREGION,
    SHIPPOSTALCODE,
    SHIPCOUNTRY,
    FREIGHT,
    SHIPVIA
  from {{ source('northwind','ORDERS') }}
),

stg_order_details as (
  select
    ORDERID,
    sum(QUANTITY) as quantityonorder,
    sum(QUANTITY * UNITPRICE * (1 - DISCOUNT)) as totalorderamount
  from {{ source('northwind','ORDER_DETAILS') }}
  group by ORDERID
),

stg_shippers as (
  select * from {{ source('northwind','SHIPPERS') }}
)

select
  o.*,
  s.COMPANYNAME as shippercompanyname,
  od.quantityonorder,
  od.totalorderamount,
  (o.shippeddatekey - o.orderdatekey) as daysfromordertoshipped,
  (o.requireddatekey - o.orderdatekey) as daysfromordertorequired,
  (o.shippeddatekey - o.requireddatekey) as shippedtorequireddelta,
  case when (o.shippeddatekey - o.requireddatekey) <= 0 then 'Y' else 'N' end as shippedontime
from stg_orders o
join stg_order_details od on o.orderid = od.orderid
join stg_shippers s on s.SHIPPERID = o.SHIPVIA
