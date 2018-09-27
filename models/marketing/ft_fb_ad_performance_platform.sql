with ads as (

  select * from {{ref('staging_fb_ads')}}

), insights as (

  select * from {{ref('b_fb_ad_insights')}}

), campaigns as (

  select * from {{ref('b_fb_campaigns')}}

), adsets as (

  select * from {{ref('staging_fb_adsets')}}

), final as (

  select
    c.name as campaign_name
    ,s.name as adset_name
    ,a.name as ad_name
    ,ai.date_day as campaign_date
    ,ai.account_name as account_name
    ,ai.account_currency as account_currency
    ,ai.dim_store_fk as dim_store_fk
    ,'facebook_br' as data_source
    ,'Paid Social' as group_channel
    ,'paid_social' as medium
    ,'facebook' as "source"
    ,sum(impressions) as impressions
    ,sum(clicks) as clicks
    ,sum(unique_clicks) as unique_clicks
    ,sum(spend) as spend
    ,sum(spend_usd) as spend_usd
  from insights ai
  left join campaigns c on c.id = ai.campaign_id
  left join adsets s on s.id = adset_id
  left join ads a on a.id = ai.ad_id
  group by 1,2,3,4,5,6,7
)

select
  *
from final
