{{
    config(
        materialized='view'
    )
}}

select 
    '{"id": 1, "book_id": 6, "quantity": 1, "order_date": "2024-12-02"}' as records

union all

select 
    '{"id": 2, "book_id": 1, "quantity": 1, "order_date": "2024-12-06"}' as records

union all

select 
    '{"id": 3, "book_id": 1, "quantity": 1, "order_date": "2025-12-24"}' as records

union all

select 
    '{"id": 4, "book_id": 3, "quantity": 1, "order_date": "2025-01-01"}' as records