with stg_suppliers as (
  select * from {{ source('northwind','SUPPLIERS') }}
)

select
  {{ dbt_utils.generate_surrogate_key(['stg_suppliers.supplierid']) }} as supplierkey,
  stg_suppliers.supplierid,
  stg_suppliers.companyname,
  stg_suppliers.contactname,
  stg_suppliers.contacttitle,
  stg_suppliers.address as supplieraddress,
  stg_suppliers.city as suppliercity,
  stg_suppliers.region as supplierregion,
  stg_suppliers.postalcode as supplierpostalcode,
  stg_suppliers.country as suppliercountry,
  stg_suppliers.phone as supplierphone,
  stg_suppliers.fax as supplierfax,
  stg_suppliers.homepage as supplierhomepage
from stg_suppliers