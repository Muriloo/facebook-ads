with fb_keyword_performance as (

    select * from {{ref('staging_fb_ad_insights')}}

),

fb_keyword_performance_agg as (

    select
        date_day as campaign_date,
        account_currency,
        url_host,
        url_path,
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        utm_term,
        'facebook ads' as platform,
        sum(clicks) as clicks,
        sum(unique_clicks) as unique_clicks,
        sum(impressions) as impressions,
        sum(spend) as spend,
        sum(spend_usd) as spend_usd
    from fb_keyword_performance
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9

)

select * from fb_keyword_performance_agg
