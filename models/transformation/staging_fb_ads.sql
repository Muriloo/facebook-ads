with ads_xf_ads as (

  select * from {{ref('b_fb_ads')}}

), ads_xf_windowed as (

  select
    *,
    count(*) over (partition by id) as num_versions,
    row_number() over (partition by id order by updated_at) as version_number
  from ads_xf_ads

), final_t as (
  select
    *,
    case
      when version_number = 1 then created_at
      when version_number > 1 then updated_at
    end as effective_from,
    case
      when version_number = num_versions then null
      else lead(updated_at) over (partition by id order by updated_at)
    end as effective_to,
    md5(id || '|' || version_number) as unique_id
  from ads_xf_windowed
)
select *
from final_t
where num_versions = version_number
