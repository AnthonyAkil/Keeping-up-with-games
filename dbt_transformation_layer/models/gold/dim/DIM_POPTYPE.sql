SELECT 
    ID                  AS "Popularity type ID",
    NAME                AS "Popularity type name"
FROM {{ ref('CLEAN_POPTYPE') }}