{% macro stitch_fb_ads() %}

    {{ adapter_macro('facebook_ads.stitch_fb_ads') }}

{% endmacro %}

--there are multiple records for different statuses, but we don't need them for our purposes yet.
--if this table needs to be expended to include multiple rows per id, this will break downstream queries that depend
--on uniqueness on this id when joins are done.

{% macro default__stitch_fb_ads() %}

select distinct
  nullif(id,'') as id,
  nullif(account_id,'') as account_id,
  nullif(adset_id,'') as adset_id,
  nullif(campaign_id,'') as campaign_id,
  nullif(name,'') as name,
  nullif(creative__id,'') as creative_id,
  created_time as created_at,
  updated_time as updated_at
from
  {{ var('account_1_schema') }}.{{ var('ads_table') }}

UNION ALL

select distinct
  nullif(id,'') as id,
  nullif(account_id,'') as account_id,
  nullif(adset_id,'') as adset_id,
  nullif(campaign_id,'') as campaign_id,
  nullif(name,'') as name,
  nullif(creative__id,'') as creative_id,
  created_time as created_at,
  updated_time as updated_at
from
  {{ var('account_2_schema') }}.{{ var('ads_table') }}

{% endmacro %}


{% macro snowflake__stitch_fb_ads() %}

select distinct
  nullif(id,'') as id,
  nullif(account_id,'') as account_id,
  nullif(adset_id,'') as adset_id,
  nullif(campaign_id,'') as campaign_id,
  nullif(name,'') as name,
  nullif(creative['id'],'')::bigint as creative_id,
  created_time as created_at,
  updated_time as updated_at
from
  {{ var('account_1_schema') }}.{{ var('ads_table') }}

UNION ALL

select distinct
  nullif(id,'') as id,
  nullif(account_id,'') as account_id,
  nullif(adset_id,'') as adset_id,
  nullif(campaign_id,'') as campaign_id,
  nullif(name,'') as name,
  nullif(creative['id'],'')::bigint as creative_id,
  created_time as created_at,
  updated_time as updated_at
from
  {{ var('account_2_schema') }}.{{ var('ads_table') }}

{% endmacro %}
