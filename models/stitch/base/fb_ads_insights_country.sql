select
  date_start::date as date_day,
  nullif(account_id,'') as account_id,
  nullif(account_name,'') as account_name,
  nullif(ad_id,'') as ad_id,
  nullif(adset_id,'') as adset_id,
  nullif(campaign_id,'') as campaign_id,
  nullif(country,'') as country,
  impressions,
  unique_impressions,
  clicks,
  unique_clicks,
  spend,
  frequency,
  reach,
  nullif(objective,'') as objective,
  app_store_clicks,
  call_to_action_clicks,
  deeplink_clicks,
  canvas_avg_view_percent,
  canvas_avg_view_time,
  inline_link_clicks,
  inline_post_engagement,
  social_clicks,
  social_impressions,
  social_reach,
  total_action_value,
  total_actions,
  total_unique_actions,
  unique_inline_link_clicks,
  unique_social_clicks,
  unique_social_impressions,
  website_clicks
from
  {{ var('ads_insights_country_table') }}
