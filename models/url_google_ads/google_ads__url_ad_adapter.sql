{{ config(enabled=var('api_source') == 'google_ads') }}

with stats as (

    select *
    from {{ var('ad_stats') }}

), accounts as (

    select *
    from {{ var('account') }}
    
), campaigns as (

    select *
    from {{ var('campaign_history') }}
    where is_most_recent_record = True
    
), ad_groups as (

    select *
    from {{ var('ad_group_history') }}
    where is_most_recent_record = True
    
), ads as (

    select *
    from {{ var('ad_history') }}
    where is_most_recent_record = True
    
), final_url as (

    select *
    from {{ var('ad_final_url_history') }}
    where is_most_recent_record = True
    
), fields as (

    select
        stats.date_day,
        stats.source_relation,
        accounts.account_name,
        accounts.account_id,
        campaigns.campaign_name,
        campaigns.campaign_id,
        ad_groups.ad_group_name,
        ad_groups.ad_group_id,
        final_url.base_url,
        final_url.url_host,
        final_url.url_path,
        final_url.utm_source,
        final_url.utm_medium,
        final_url.utm_campaign,
        final_url.utm_content,
        final_url.utm_term,
        sum(stats.spend) as spend,
        sum(stats.clicks) as clicks,
        sum(stats.impressions) as impressions

        {% for metric in var('google_ads__ad_stats_passthrough_metrics') %}
        , sum(stats.{{ metric }}) as {{ metric }}
        {% endfor %}

    from stats
    left join ads
        on stats.ad_id = ads.ad_id
        and stats.source_relation = ads.source_relation
    left join final_url
        on ads.ad_id = final_url.ad_id
        and ads.source_relation = final_url.source_relation
    left join ad_groups
        on ads.ad_group_id = ad_groups.ad_group_id
        and ads.source_relation = ad_groups.source_relation
    left join campaigns
        on ad_groups.campaign_id = campaigns.campaign_id
        and ad_groups.source_relation = campaigns.source_relation
    left join accounts
        on campaigns.account_id = accounts.account_id
        and campaigns.source_relation = accounts.source_relation
    {{ dbt_utils.group_by(16) }}

)

select *
from fields