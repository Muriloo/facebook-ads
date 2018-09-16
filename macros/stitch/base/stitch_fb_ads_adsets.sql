{% macro stitch_fb_ads_adsets() %}

    {{ adapter_macro('facebook_ads.stitch_fb_ads_adsets') }}

{% endmacro %}


{% macro default__stitch_fb_ads_adsets() %}

select
  id,
  nullif(name,'') as name,
  nullif(account_id,'') as account_id,
  nullif(campaign_id,'') as campaign_id,
  created_time,
  nullif(effective_status,'') as effective_status
from
  {{ var('account_1_schema') }}.{{ var('adsets_table') }}

UNION ALL

select
  id,
  nullif(name,'') as name,
  nullif(account_id,'') as account_id,
  nullif(campaign_id,'') as campaign_id,
  created_time,
  nullif(effective_status,'') as effective_status
from
  {{ var('account_2_schema') }}.{{ var('adsets_table') }}

{% endmacro %}
