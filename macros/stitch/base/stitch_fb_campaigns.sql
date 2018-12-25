{% macro stitch_fb_ads_campaigns() %}

    {{ adapter_macro('facebook_ads.stitch_fb_ads_campaigns') }}

{% endmacro %}


{% macro default__stitch_fb_ads_campaigns() %}

WITH united_tables as (
  {{ dbt_utils.union_tables(
      tables=[var('account_1_schema') ~ "." ~ var('campaigns_table')
              ,var('account_2_schema') ~ "." ~ var('campaigns_table')
              ,var('account_3_schema') ~ "." ~ var('campaigns_table')
              ,var('account_4_schema') ~ "." ~ var('campaigns_table')
              ,var('account_5_schema') ~ "." ~ var('campaigns_table')
              ]
  ) }}
)

select
  nullif(id,'') as id,
  nullif(name,'') as name
from
  united_tables

{% endmacro %}
