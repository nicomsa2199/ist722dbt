with stg_employees as (
  select * from {{ source('northwind','EMPLOYEES') }}
),
stg_supervisors as (
  select * from {{ source('northwind','EMPLOYEES') }}
)
select
  {{ dbt_utils.generate_surrogate_key(['e.EMPLOYEEID']) }} as employeekey,
  e.EMPLOYEEID as employeeid,
  concat(e.LASTNAME, ', ', e.FIRSTNAME) as employeenamelastfirst,
  concat(e.FIRSTNAME, ' ', e.LASTNAME) as employeenamefirstlast,
  e.TITLE as employeetitle,
  concat(s.LASTNAME, ', ', s.FIRSTNAME) as supervisornamelastfirst,
  concat(s.FIRSTNAME, ' ', s.LASTNAME) as supervisornamefirstlast
from stg_employees e
left join stg_supervisors s
  on e.REPORTSTO = s.EMPLOYEEID
  