{{ config(enabled=var('api_source') == 'adwords') }}

with base as (

    select *
    from {{ var('criteria_performance') }}

), subbase as (

    select *
    from {{ var('final_url_performance') }}

), fields as (

    select
        subbase.date_day,
        subbase.account_name,
        subbase.external_customer_id,
        subbase.campaign_name,
        subbase.campaign_id,
        subbase.ad_group_name,
        subbase.ad_group_id,
        subbase.base_url,
        subbase.url_host,
        subbase.url_path,
        CASE
            WHEN LOWER(subbase.utm_source) like CONCAT('%', LOWER(coalesce(base.criteria_type, '')) ,'%') then REPLACE(subbase.utm_source,CONCAT('{', LOWER(coalesce(base.criteria_type, '')) ,'}'), base.criteria)
            else subbase.utm_source
        end AS utm_source,
        CASE
            WHEN LOWER(subbase.utm_medium) like CONCAT('%', LOWER(coalesce(base.criteria_type, '')) ,'%') then REPLACE(subbase.utm_medium,CONCAT('{', LOWER(coalesce(base.criteria_type, '')) ,'}'), base.criteria)
            else subbase.utm_medium
        end AS utm_medium,
        CASE
            WHEN LOWER(subbase.utm_campaign) like CONCAT('%', LOWER(coalesce(base.criteria_type, '')) ,'%') then REPLACE(subbase.utm_campaign,CONCAT('{', LOWER(coalesce(base.criteria_type, '')) ,'}'), base.criteria)
            else subbase.utm_campaign
        end AS utm_campaign,
        CASE
            WHEN LOWER(subbase.utm_content) like CONCAT('%', LOWER(coalesce(base.criteria_type, '')) ,'%') then REPLACE(subbase.utm_content,CONCAT('{', LOWER(coalesce(base.criteria_type, '')) ,'}'), base.criteria)
            else subbase.utm_content
        end AS utm_content,
        CASE
            WHEN LOWER(subbase.utm_term) like CONCAT('%', LOWER(coalesce(base.criteria_type, '')) ,'%') then REPLACE(subbase.utm_term,CONCAT('{', LOWER(coalesce(base.criteria_type, '')) ,'}'), base.criteria)
            else subbase.utm_term
        end AS utm_term,
        sum(base.spend) as spend,
        sum(base.clicks) as clicks,
        sum(base.impressions) as impressions

        {% for metric in var('google_ads__url_passthrough_metrics') %}
        , sum({{ metric }}) as {{ metric }}
        {% endfor %}
    from base
    left join subbase on cast(base.campaign_id as int64)  = cast(subbase.campaign_id as int64) and
        cast(base.ad_group_id as int64) = cast(subbase.ad_group_id as int64) and cast(base.date_day as string) = cast(subbase.date_day as string)
    {{ dbt_utils.group_by(15) }}

)

select *
from fields