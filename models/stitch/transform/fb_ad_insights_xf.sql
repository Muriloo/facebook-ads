with ads as (

  select * from {{ref('fb_ads')}}

), creatives as (

  select * from {{ref('fb_ad_creatives')}}

), insights as (

  select * from {{ref('fb_ad_insights')}}

)
select
  md5(insights.date_day || '|' || insights.ad_id) as id,
  insights.*,
  creatives.base_url,
  creatives.url,
  creatives.utm_medium,
  creatives.utm_source,
  creatives.utm_campaign,
  creatives.utm_content,
  creatives.utm_term
from insights
  left outer join ads on insights.ad_id = ads.id
  left outer join creatives on ads.creative_id = creatives.id
  --these have to be an outer join because while the stitch integration goes back in time for the core reporting tables (insights, etc),
  --it doesn't go back in time for the lookup tables. so there are actually lots of ad_ids that don't exist when you try to do the join,
  --but they're always prior to the date you initially made the connection.
