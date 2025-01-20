{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

/*
set the json fields that we want to extract
*/
{% set json_fields = [
    {'name': 'order_id', 'type': 'int64', 'sources': ['id']},
    {'name': 'book_id', 'type': 'int64'},
    {'name': 'quantity', 'type': 'int64'},
    {'name': 'order_date', 'type': 'date'}
] %}

with extracted as (
    select
        *
    from
        {{ ref('stg_book_orders') }}
    where
        true
        {{ filter_latest(order_date, order_date, date) }}
),
/*
extracting json fields in json_field to individual columns
using for loop and if else
*/
flattened as (
    select
        {% for field in json_fields -%}
            {%- if field.type == 'json' -%}
                {% set extract_function = 'json_extract' %}
            {%- else -%}
                {% set extract_function = 'json_extract_scalar' %}
            {%- endif -%}
            {% if field.sources -%}
                coalesce(
                    {% for source in field.sources -%}
                        {% if field.type == 'json' -%}
                            json_extract(records, '$.{{ source }}')
                        {% elif field.type == 'json-string' -%}
                            json_extract_scalar(json_extract_scalar(records, '$.{{ source[0] }}'), '$.{{ source[1] }}')
                        {% else -%}
                            json_extract_scalar(records, '$.{{ source }}')
                        {% endif %}{{ ',' if not loop.last }}
                    {% endfor %}
                )
            {% else -%}
                {{ extract_function }}(records, '$.{{ field.name }}')
            {%- endif %} as {{ field.name }}{{ ',' if not loop.last }}
        {% endfor -%}
    from
        extracted
),
/*
casting extracted json fields in json_field to their respective data types
using for loop and if else
*/
casted as (
    select
        {% for field in json_fields -%}
            {%- if field.type == 'json-string' -%}
                {% set field_type = field.subtype %}
            {%- else -%}
                {% set field_type = field.type %}
            {%- endif -%}
            {% if field_type == 'boolean' -%}
                cast(
                    case
                        when {{ field.name }} = '1' then 'true'
                        when {{ field.name }} = '0' then 'false'
                        when {{ field.name }} is null then '{{ field.default if field.default else "false" }}'
                        else {{ field.name }}
                    end
                 as {{ field_type }}) as {{ field.name }},
            {% elif field_type == 'string' -%}
                coalesce(cast({{ field.name }} as {{ field_type }}), '-') as {{ field.name }},
            {% elif field_type in ('int64', 'numeric') -%}
                coalesce(cast({{ field.name }} as {{ field_type }}), 0) as {{ field.name }},
            {% elif field_type == 'timestamp' -%}
                cast(
                    coalesce(
                        case
                            {# /* invalid value: '-0001-11-30T00:00:00+07:07' */ #}
                            when starts_with({{ field.name }}, '-') then null
                            else {{ field.name }}
                        end,
                        '{{ field.default if field.default else "1990-01-01" }}'
                    ) as {{ field_type }}
                ) as {{ field.name }},
            {% elif field_type == 'json' -%}
                coalesce(to_json_string({{ field.name }}), '-') {{ field.name }},
            {% else -%}
                cast({{ field.name }} as {{ field_type }}) {{ field.name }},
            {% endif -%}
        {% endfor %}
    from
        flattened
)
select
    *
from
    casted