with actions as (

  select * from {{ var('ads_insights_actions_values_table') }}

)

select
  _sdc_level_0_id as id,
  _sdc_source_key_ad_id as ad_id,
  _sdc_source_key_adset_id as adset_id,
  _sdc_source_key_campaign_id as campaign_id,
  _sdc_source_key_date_start::date as date_day,
  action_destination,
  action_type,
  value as action_value
from actions
