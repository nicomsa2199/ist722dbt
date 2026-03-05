with f as (
  select * from {{ ref('fact_sales') }}
),
c as (
  select * from {{ ref('dim_customer') }}
),
e as (
  select * from {{ ref('dim_employee') }}
),
d as (
  select * from {{ ref('dim_date') }}
),
p as (
  select * from {{ ref('dim_product') }}
),
s as (
  select * from {{ ref('dim_supplier') }}
)

select
  f.*,

  c.customerid,
  c.companyname as customer_companyname,
  c.contactname as customer_contactname,
  c.contacttitle as customer_contacttitle,
  c.address as customer_address,
  c.city as customer_city,
  c.region as customer_region,
  c.postalcode as customer_postalcode,
  c.country as customer_country,
  c.phone as customer_phone,
  c.fax as customer_fax,

  e.employeeid,
  e.employeenamelastfirst,
  e.employeenamefirstlast,
  e.employeetitle,
  e.supervisornamelastfirst,
  e.supervisornamefirstlast,

  d.date,
  d.year,
  d.month,
  d.quarter,
  d.day,
  d.dayname,
  d.monthname,

  p.productid,
  p.productname,
  p.categoryname,
  p.categorydescription,

  s.supplierid,
  s.companyname as supplier_companyname,
  s.contactname as supplier_contactname,
  s.contacttitle as supplier_contacttitle,
  s.supplieraddress,
  s.suppliercity,
  s.supplierregion,
  s.supplierpostalcode,
  s.suppliercountry,
  s.supplierphone,
  s.supplierfax,
  s.supplierhomepage

from f
left join c on f.customerkey = c.customerkey
left join e on f.employeekey = e.employeekey
left join d on f.orderdatekey = d.datekey
left join p on f.productkey = p.productkey
left join s on p.supplierkey = s.supplierkey