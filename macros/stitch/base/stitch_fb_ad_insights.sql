{% macro stitch_fb_ad_insights() %}

    {{ adapter_macro('facebook_ads.stitch_fb_ad_insights') }}

{% endmacro %}


{% macro default__stitch_fb_ad_insights() %}

select
  date_start::date as date_day,
  nullif(account_id,'') as account_id,
  nullif(account_name,'') as account_name,
  nullif(ad_id,'') as ad_id,
  nullif(adset_id,'') as adset_id,
  nullif(campaign_id,'') as campaign_id,
  impressions,
  clicks,
  unique_clicks,
  spend,
  -- frequency,
  reach,
  '{{ var("account_1_currency")}}' as account_currency,
  -- spend*fx.fx_rate as spend_usd, -- @commenthere
  {{ var("account_1_store_id")}} as dim_store_fk
  -- nullif(objective,'') as objective,
  -- canvas_avg_view_percent,
  -- canvas_avg_view_time,
  -- inline_link_clicks,
  -- inline_post_engagement,
  -- unique_inline_link_clicks
from
  {{ var('account_1_schema') }}.{{ var('ads_insights_table') }}
-- left join {{ref('map_fx_rate')}} fx on fx.currency_from = '{{ var("account_1_currency")}}' and fx.currency_to = 'USD' -- @commenthere

UNION ALL

select
  date_start::date as date_day,
  nullif(account_id,'') as account_id,
  nullif(account_name,'') as account_name,
  nullif(ad_id,'') as ad_id,
  nullif(adset_id,'') as adset_id,
  nullif(campaign_id,'') as campaign_id,
  impressions,
  clicks,
  unique_clicks,
  spend,
  -- frequency,
  reach,
  '{{ var("account_2_currency")}}' as account_currency,
  -- spend*fx.fx_rate as spend_usd, -- @commenthere
  {{ var("account_2_store_id")}} as dim_store_fk
  -- nullif(objective,'') as objective,
  -- canvas_avg_view_percent,
  -- canvas_avg_view_time,
  -- inline_link_clicks,
  -- inline_post_engagement,
  -- unique_inline_link_clicks
from
  {{ var('account_2_schema') }}.{{ var('ads_insights_table') }}
-- left join {{ref('map_fx_rate')}} fx on fx.currency_from = '{{ var("account_2_currency")}}' and fx.currency_to = 'USD' -- @commenthere

UNION ALL

select
  date_start::date as date_day,
  nullif(account_id,'') as account_id,
  nullif(account_name,'') as account_name,
  nullif(ad_id,'') as ad_id,
  nullif(adset_id,'') as adset_id,
  nullif(campaign_id,'') as campaign_id,
  impressions,
  clicks,
  unique_clicks,
  spend,
  -- frequency,
  reach,
  '{{ var("account_3_currency")}}' as account_currency,
  -- spend*fx.fx_rate as spend_usd, -- @commenthere
  {{ var("account_3_store_id")}} as dim_store_fk
  -- nullif(objective,'') as objective,
  -- canvas_avg_view_percent,
  -- canvas_avg_view_time,
  -- inline_link_clicks,
  -- inline_post_engagement,
  -- unique_inline_link_clicks
from
  {{ var('account_3_schema') }}.{{ var('ads_insights_table') }}
-- left join {{ref('map_fx_rate')}} fx on fx.currency_from = '{{ var("account_3_currency")}}' and fx.currency_to = 'USD' -- @commenthere

UNION ALL

select
  date_start::date as date_day,
  nullif(account_id,'') as account_id,
  nullif(account_name,'') as account_name,
  nullif(ad_id,'') as ad_id,
  nullif(adset_id,'') as adset_id,
  nullif(campaign_id,'') as campaign_id,
  impressions,
  clicks,
  unique_clicks,
  spend,
  -- frequency,
  reach,
  '{{ var("account_4_currency")}}' as account_currency,
  -- spend*fx.fx_rate as spend_usd, -- @commenthere
  {{ var("account_4_store_id")}} as dim_store_fk
  -- nullif(objective,'') as objective,
  -- canvas_avg_view_percent,
  -- canvas_avg_view_time,
  -- inline_link_clicks,
  -- inline_post_engagement,
  -- unique_inline_link_clicks
from
  {{ var('account_4_schema') }}.{{ var('ads_insights_table') }}
-- left join {{ref('map_fx_rate')}} fx on fx.currency_from = '{{ var("account_4_currency")}}' and fx.currency_to = 'USD' -- @commenthere

UNION ALL

select
  date_start::date as date_day,
  nullif(account_id,'') as account_id,
  nullif(account_name,'') as account_name,
  nullif(ad_id,'') as ad_id,
  nullif(adset_id,'') as adset_id,
  nullif(campaign_id,'') as campaign_id,
  impressions,
  clicks,
  unique_clicks,
  spend,
  -- frequency,
  reach,
  '{{ var("account_5_currency")}}' as account_currency,
  -- spend*fx.fx_rate as spend_usd, -- @commenthere
  {{ var("account_5_store_id")}} as dim_store_fk
  -- nullif(objective,'') as objective,
  -- canvas_avg_view_percent,
  -- canvas_avg_view_time,
  -- inline_link_clicks,
  -- inline_post_engagement,
  -- unique_inline_link_clicks
from
  {{ var('account_5_schema') }}.{{ var('ads_insights_table') }}
-- left join {{ref('map_fx_rate')}} fx on fx.currency_from = '{{ var("account_5_currency")}}' and fx.currency_to = 'USD' -- @commenthere

{% endmacro %}


{% macro snowflake__stitch_fb_ad_insights() %}

select
  date_start::date as date_day,
  account_id,
  account_name as account_name,
  ad_id,
  adset_id,
  campaign_id,
  impressions,
  clicks,
  unique_clicks,
  spend,
  -- frequency,
  reach,
  '{{ var("account_1_currency")}}' as account_currency,
  -- spend*fx.fx_rate as spend_usd, -- @commenthere
  {{ var("account_1_store_id")}} as dim_store_fk
  -- nullif(objective,'') as objective,
  -- canvas_avg_view_percent,
  -- canvas_avg_view_time,
  -- inline_link_clicks,
  -- inline_post_engagement,
  -- unique_inline_link_clicks
from {{ var('account_1_schema') }}.{{ var('ads_insights_table') }}
-- left join {{ref('map_fx_rate')}} fx on fx.currency_from = '{{ var("account_1_currency")}}' and fx.currency_to = 'USD' -- @commenthere

UNION ALL

select
  date_start::date as date_day,
  account_id,
  account_name as account_name,
  ad_id,
  adset_id,
  campaign_id,
  impressions,
  clicks,
  unique_clicks,
  spend,
  -- frequency,
  reach,
  '{{ var("account_2_currency")}}' as account_currency,
  -- spend*fx.fx_rate as spend_usd, -- @commenthere
  {{ var("account_2_store_id")}} as dim_store_fk
  -- nullif(objective,'') as objective,
  -- canvas_avg_view_percent,
  -- canvas_avg_view_time,
  -- inline_link_clicks,
  -- inline_post_engagement,
  -- unique_inline_link_clicks
from {{ var('account_2_schema') }}.{{ var('ads_insights_table') }}
-- left join {{ref('map_fx_rate')}} fx on fx.currency_from = '{{ var("account_2_currency")}}' and fx.currency_to = 'USD' -- @commenthere

UNION ALL

select
  date_start::date as date_day,
  account_id,
  account_name as account_name,
  ad_id,
  adset_id,
  campaign_id,
  impressions,
  clicks,
  unique_clicks,
  spend,
  -- frequency,
  reach,
  '{{ var("account_3_currency")}}' as account_currency,
  -- spend*fx.fx_rate as spend_usd, -- @commenthere
  {{ var("account_3_store_id")}} as dim_store_fk
  -- nullif(objective,'') as objective,
  -- canvas_avg_view_percent,
  -- canvas_avg_view_time,
  -- inline_link_clicks,
  -- inline_post_engagement,
  -- unique_inline_link_clicks
from {{ var('account_3_schema') }}.{{ var('ads_insights_table') }}
-- left join {{ref('map_fx_rate')}} fx on fx.currency_from = '{{ var("account_3_currency")}}' and fx.currency_to = 'USD' -- @commenthere


UNION ALL

select
  date_start::date as date_day,
  account_id,
  account_name as account_name,
  ad_id,
  adset_id,
  campaign_id,
  impressions,
  clicks,
  unique_clicks,
  spend,
  -- frequency,
  reach,
  '{{ var("account_4_currency")}}' as account_currency,
  -- spend*fx.fx_rate as spend_usd, -- @commenthere
  {{ var("account_4_store_id")}} as dim_store_fk
  -- nullif(objective,'') as objective,
  -- canvas_avg_view_percent,
  -- canvas_avg_view_time,
  -- inline_link_clicks,
  -- inline_post_engagement,
  -- unique_inline_link_clicks
from {{ var('account_4_schema') }}.{{ var('ads_insights_table') }}
-- left join {{ref('map_fx_rate')}} fx on fx.currency_from = '{{ var("account_4_currency")}}' and fx.currency_to = 'USD' -- @commenthere


UNION ALL

select
  date_start::date as date_day,
  account_id,
  account_name as account_name,
  ad_id,
  adset_id,
  campaign_id,
  impressions,
  clicks,
  unique_clicks,
  spend,
  -- frequency,
  reach,
  '{{ var("account_5_currency")}}' as account_currency,
  -- spend*fx.fx_rate as spend_usd, -- @commenthere
  {{ var("account_5_store_id")}} as dim_store_fk
  -- nullif(objective,'') as objective,
  -- canvas_avg_view_percent,
  -- canvas_avg_view_time,
  -- inline_link_clicks,
  -- inline_post_engagement,
  -- unique_inline_link_clicks
from {{ var('account_5_schema') }}.{{ var('ads_insights_table') }}
-- left join {{ref('map_fx_rate')}} fx on fx.currency_from = '{{ var("account_5_currency")}}' and fx.currency_to = 'USD' -- @commenthere
{% endmacro %}
