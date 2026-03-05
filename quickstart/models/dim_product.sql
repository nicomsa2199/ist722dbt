with p as (
  select * from {{ source('northwind','PRODUCTS') }}
),
c as (
  select * from {{ source('northwind','CATEGORIES') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['p.productid']) }} as productkey,
  p.productid,
  p.productname,

  {{ dbt_utils.generate_surrogate_key(['p.supplierid']) }} as supplierkey,

  c.categoryname,
  c.description as categorydescription
from p
left join c on p.categoryid = c.categoryid