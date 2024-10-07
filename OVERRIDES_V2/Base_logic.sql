WITH the view... (

),
OVERRIDE_VAL AS (
    -- most recent records
    SELECT
        HASH_DIFF
        ,ATTRIBUTE_NAME
        ,ATTRIBUTE_VALUE
        ,OVERRIDE_TYPE
        ,BUSINESS_DATE
        ,LOAD_DATETIME
        ,TABLE_NAME
    FROM
        OVERRIDE_LOOKUP
    WHERE
        BUSINESS_DATE = '...' -- MATCH THE DATE
)
SELECT
    v.HASH_DIFF
    ,v.BUSINESS_DATE
    ,v.LOAD_DATETIME
    ,v.TABLE_NAME
    CASE
        WHEN o.OVERRIDE_TYPE = 'Override'
             AND o.LOAD_DATETIME > v.LOAD_DATETIME
        THEN o.ATTRIBUTE_VALUE
        ELSE v.ATTRIBUTE_VALUE
    END AS ATTRIBUTE_VALUE,
    v.ATTRIBUTE_NAME
FROM
    view_data v
LEFT JOIN
    overrides o
    ON v.HASH_DIFF = o.HASH_DIFF
    AND v.ATTRIBUTE_NAME = o.ATTRIBUTE_NAME
    AND v.TABLE_NAME = o.TABLE_NAME
    AND o.OVERRIDE_TYPE = 'Override'
WHERE
    NOT EXISTS(
        -- Exclude rows that should be removed based on 'Exclude' type overrides
        SELECT 1
        FROM
            overrides o2
        WHERE
            o2.HASH_DIFF = v.HASH_DIFF
        AND o2.OVERRIDE_TYPE = 'Exclude'
        AND o2.BUSINESS_DATE = v.BUSINESS_DATE
    );