with stg_customers as (
  select * 
  from {{ source('northwind','CUSTOMERS') }}
)
select
  {{ dbt_utils.generate_surrogate_key(['stg_customers.CUSTOMERID']) }} as customerkey,
  stg_customers.*
from stg_customers
