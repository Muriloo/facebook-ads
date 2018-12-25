{% macro stitch_fb_ads_adsets() %}

    {{ adapter_macro('facebook_ads.stitch_fb_ads_adsets') }}

{% endmacro %}


{% macro default__stitch_fb_ads_adsets() %}

WITH united_tables as (
  {{ dbt_utils.union_tables(
      tables=[var('account_1_schema') ~ "." ~ var('adsets_table')
              ,var('account_2_schema') ~ "." ~ var('adsets_table')
              ,var('account_3_schema') ~ "." ~ var('adsets_table')
              ,var('account_4_schema') ~ "." ~ var('adsets_table')
              ,var('account_5_schema') ~ "." ~ var('adsets_table')
              ]
  ) }}
)
select
  id,
  nullif(name,'') as name,
  nullif(account_id,'') as account_id,
  nullif(campaign_id,'') as campaign_id,
  created_time,
  nullif(effective_status,'') as effective_status
from
  united_tables

{% endmacro %}
