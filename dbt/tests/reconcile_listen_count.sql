select 1 as mismatch
where (select count(*) from {{ ref('fact_listens') }}) <>
      (select count(*) from {{ source('silver', 'events') }} where event_type = 'listen')
