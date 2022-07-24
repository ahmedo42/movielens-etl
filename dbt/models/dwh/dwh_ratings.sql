{{
    config(materialized='table',
        cluster_by = "movieId",
        schema='dwh'
    )
}}

SELECT *
FROM {{ source('dwh', 'ratings') }}