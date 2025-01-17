{{
    config(
        materialized='view'
    )
}}

select 
    '{"id": 1, "name": "The Russian", "type": "fiction", "available": true}' as records

union all

select 
    '{"id": 3, "name": "The Vanishing Half", "type": "fiction", "available": true}' as records

union all

select 
    '{"id": 4, "name": "The Midnight Library", "type": "fiction", "available": true}' as records

union all

select 
    '{"id": 6, "name": "Viscount Who Loved Me", "type": "fiction", "available": true}' as records