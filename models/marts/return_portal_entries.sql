with

return_portal_entries as (
    select * from {{ ref('stg_magento__return_portal_entries') }}
)

select * from return_portal_entries
