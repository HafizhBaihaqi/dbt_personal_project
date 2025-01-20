{% macro filter_latest(source_column, target_column, date_type) %}
{# this macro is used for incremental filter to only fetch the latest data from the upstream model based on this #}
{% if not target_column -%}
    {# if target_column not set, then take from source_column #}
    {% set target_column = source_column %}
{% endif %}
{% if not date_type -%}
    {# if date_type not set, then the default would be timestamp #}
    {% set date_type = 'timestamp' %}
{% endif %}
{% if is_incremental() -%}
    {# incremental filter starts here #}
    {# this incremental filter is able to backload or backfill data from a certain period of time #}
    {# 1. set variables for backload (true or false) #}
    {%- set backload = var("backload", "") %}
    {# 2. set variables for date_start (timestamp or date) #}
    {%- set date_start = var("date_start", "") %}
    {# 3. set variables for date_until (timestamp or date) #}
    {%- set date_until = var("date_until", "") %}
    {% if backload %}
        {# if backload is true, then take value from date_start and date_until that has been set #}
        and date({{ target_column }}) >= '{{ date_start }}'
        and date({{ target_column }}) <= '{{ date_until }}'
    {% else -%}
        {# if backload is false, then filter to only fetch data from latest #}
        {% if not latest -%}
            {%- call statement('latest', fetch_result=True) -%}
                select max({{ source_column }}) from {{ this }}
            {%- endcall -%}
        {% endif %}
        {%- set latest = load_result('latest') -%}
        and  {{ date_type }}({{ target_column }}) >= {{ date_type }}('{{ latest["data"][0][0] }}')
    {% endif %}
{% endif %}
{% endmacro %}
