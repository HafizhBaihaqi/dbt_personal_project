{{
    config(
        materialized='view'
    )
}}

select 
    '{"fiction":{"id": 1, "name": "The Russian", "available": true}}' AS records

union all

select 
    '{"non-fiction":{"id": 2, "name": "Just as I Am", "available": false}}' AS records

union all

select 
    '{"fiction":{"id": 3, "name": "The Vanishing Half", "available": true}}' AS records

union all

select 
    '{"fiction":{"id": 4, "name": "The Midnight Library", "available": true}}' AS records

union all

select 
    '{"non-fiction":{"id": 5, "name": "Untamed", "available": true}}' AS records

union all

select 
    '{"fiction":{"id": 6, "name": "Viscount Who Loved Me", "type": "fiction", "available": true}}' AS records