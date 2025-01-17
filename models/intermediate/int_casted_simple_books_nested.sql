{{
    config(
        materialized='table'
    )
}}

/*
fetch unique type of book
*/
{% set query %}
    select
        distinct replace(split(records, '{"') [offset(1)], '":','') as book_type
    from
        {{ ref('stg_simple_books_nested') }}
{% endset %}

/*
create a function to:
1. run a query of book_type
2. save the result as rows
3. create an empty list called book_type
4. looping using for-loop to iterate each row in the query result and append it to book_type for every rows in the result
*note: alias may required as the key might have '-' instead of '_' causing the query won't accept it to be the column name
**note: can use regex_replace as well instead of replace
*/
{% if execute %}
    {% set results = run_query(query) %}
    {% set result_rows = results.rows %}

    {% set book_type = [] %}

    {% for row in result_rows %}
        {% set value = row.values()[0] %}
        {% do book_type.append({'key': value, 'alias': value|replace('-', '_')}) %}
    {% endfor %}
{% endif %}

with extracted as (
    select
        *
    from
        {{ ref('stg_simple_books_nested') }}
),
/*
extracting json fields in book_type to individual columns
*/
flattened as (
    select
        {% for i in book_type -%}
            safe_cast(json_extract(records,'$.{{ i.key }}.id') as int64) as {{ i.alias }}_id,
            safe_cast(json_extract(records,'$.{{ i.key }}.name') as string) as {{ i.alias }}_name,
            safe_cast(json_extract(records,'$.{{ i.key }}.available') as boolean) as {{ i.alias }}_available,
        {% endfor %}
    from
        extracted
),
/*
combined extracted fields using coalesce 
because if there are multiple book_type, each column will be duplicated under different type
example: fiction.id, non-fiction.id -> fiction_id, non_fiction_id
*/
combined as (
    select
        coalesce(
            {% for i in book_type %}
            {{ i.alias }}_id
            {%- if not loop.last %}, {% endif -%}
            {% endfor %}
        ) as id,
        replace(
            coalesce(
                {% for i in book_type %}
                {{ i.alias }}_name
                {%- if not loop.last %}, {% endif -%}
                {% endfor %}
            ),
        '"','') as name,
        coalesce(
            {% for i in book_type %}
            {{ i.alias }}_available
            {%- if not loop.last %}, {% endif -%}
            {% endfor %}
        ) as available
    from
        flattened
)
select
    *
from
    combined