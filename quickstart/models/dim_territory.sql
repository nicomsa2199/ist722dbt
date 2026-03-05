with territories as (
    select *
    from {{ source('northwind','TERRITORIES') }}
),

regions as (
    select *
    from {{ source('northwind','REGION') }}
)

select
    territories.TERRITORYID as territorykey,
    territories.TERRITORYID as territoryid,
    territories.TERRITORYDESCRIPTION as territorydescription,
    regions.REGIONDESCRIPTION as regiondescription
from territories
left join regions
    on territories.REGIONID = regions.REGIONID
    