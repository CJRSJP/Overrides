create or replace view IM_VAULT_DEV.BDV_OVR_TEST.FUND_WRAPPER_NAV_SAT(
	FUND_WRAPPER_ID,
	FUND_WRAPPER_HKEY,
	BUSINESS_DATE,
	NAV_CALC_DATE,
	NET_ASSET_VALUE,
	NAV_PER_SHARE,
	NET_CAPSTOCK_SHARES_OUTSTANDING,
    HASH_DIFF
	LOAD_DATETIME
) as(
WITH ACFWRP AS(
  SELECT * 
, TRY_TO_DATE(BUS_DATE::VARCHAR,'YYYYMMDD') AS FROM_DATE
, IFNULL(TRY_TO_DATE(LAG(BUS_DATE) OVER (PARTITION BY FUND_WRAPPER_INSIGHTS_TICKER 
         ORDER BY TRY_TO_DATE(BUS_DATE::VARCHAR,'YYYYMMDD') DESC)::VARCHAR,'YYYYMMDD')-1, TO_DATE('99991231', 'YYYYMMDD')) AS TO_DATE_T
  ,COALESCE(NULLIF(FUND_WRAPPER_SSGS_ACCOUNT_CLOSED_DATE, ''), TO_DATE_T) TO_DATE
FROM IM_VAULT_DEV.RDV.ACCOUNT_FUND_WRAPPER_SAT  WHERE TRIM(SSGS_NAV_BASIS_IND) IN('A','T')
     --  QUALIFY ROW_NUMBER() OVER(PARTITION BY FUND_WRAPPER_ID ORDER BY FROM_DATE DESC)=1
  ORDER BY LNK_ACCOUNT_FUND_WRAPPER_BKEY
), 
 
  FWRP AS(
  SELECT * 
, TRY_TO_DATE(BUS_DATE::VARCHAR,'YYYYMMDD') AS FROM_DATE
, IFNULL(TRY_TO_DATE(LAG(BUS_DATE) OVER (PARTITION BY FUND_WRAPPER_HKEY 
         ORDER BY TRY_TO_DATE(BUS_DATE::VARCHAR,'YYYYMMDD') DESC)::VARCHAR,'YYYYMMDD')-1, TO_DATE('99991231', 'YYYYMMDD')) AS TO_DATE_T
,COALESCE(NULLIF(FUND_WRAPPER_CLOSED_DATE, ''), TO_DATE_T) TO_DATE
FROM IM_VAULT_DEV.RDV.FUND_WRAPPER_SAT ORDER BY FUND_WRAPPER_BKEY
),
FUND_WRAPPER_NAV_SAT AS (
    SELECT DISTINCT
        ACFWRP.FUND_WRAPPER_ID AS FUND_WRAPPER_ID,
        FWRP.FUND_WRAPPER_HKEY AS FUND_WRAPPER_HKEY,
        TRY_TO_DATE(NV.BUSINESS_DATE,'YYYY-MM-DD') AS BUSINESS_DATE,
        TRY_TO_DATE(NV.NAV_DATE,'YYYYMMDD') AS NAV_CALC_DATE,
        TRY_TO_NUMERIC(NV.NET_ASSET, 38, 2) AS NET_ASSET_VALUE,
        TO_NUMERIC(NV.NAV, 38, 4) AS NAV_PER_SHARE,
        TO_NUMERIC(NV.NET_SHARES_OUTST, 38, 3) AS NET_CAPSTOCK_SHARES_OUTSTANDING,
        NV.HASH_DIFF,
        NV.LOAD_DATETIME
    FROM
        "IM_VAULT_DEV"."RDV"."NAV_SSB_SAT" NV 
    JOIN
        ACFWRP ACFWRP
        ON (NV.ACCOUNT_BKEY=ACFWRP.STT_MCH_ACCOUNT_CODE
        AND NV.BASIS_IND=ACFWRP.SSGS_NAV_BASIS_IND 
        AND TO_DATE(NAV_DATE, 'YYYYMMDD')>= ACFWRP.FROM_DATE
        AND  TO_DATE(NAV_DATE, 'YYYYMMDD')<= ACFWRP.TO_DATE
        )
    JOIN
        FWRP FWRP 
        ON (ACFWRP.FUND_WRAPPER_ID=FWRP.FUND_WRAPPER_ID
        AND TO_DATE(NAV_DATE, 'YYYYMMDD')>= FWRP.FROM_DATE
        AND  TO_DATE(NAV_DATE, 'YYYYMMDD')<= FWRP.TO_DATE
        )

    UNION ALL

    SELECT DISTINCT
        ACFWRP.FUND_WRAPPER_ID AS FUND_WRAPPER_ID,
        FWRP.FUND_WRAPPER_HKEY AS FUND_WRAPPER_HKEY,
        TRY_TO_DATE(NV.NAV_DATE,'YYYYMMDD') AS BUSINESS_DATE,
        TRY_TO_DATE(NV.NAV_DATE,'YYYYMMDD') AS NAV_CALC_DATE,
        TRY_TO_NUMERIC(NV.NET_ASSET, 38, 2) AS NET_ASSET_VALUE,
        TO_NUMERIC(NV.NAV, 38, 4) AS NAV_PER_SHARE,
        TO_NUMERIC(NV.NET_SHARES_OUTST, 38, 3) AS NET_CAPSTOCK_SHARES_OUTSTANDING,
        NV.HASH_DIFF,
        TRY_TO_DATE(NV.DATE_ADDED, 'YYYYMMDD') AS "LOAD_DATE"
    FROM
        IM_VAULT_DEV.REFERENCE.NAV_MANUAL_ADD NV 
    JOIN
        ACFWRP ACFWRP
        ON (NV.FUND_ID=ACFWRP.STT_MCH_ACCOUNT_CODE
        AND NV.BASIS_IND=ACFWRP.SSGS_NAV_BASIS_IND 
        AND TO_DATE(NAV_DATE, 'YYYYMMDD')>= ACFWRP.FROM_DATE
        AND  TO_DATE(NAV_DATE, 'YYYYMMDD')<= ACFWRP.TO_DATE
        )
    JOIN
        FWRP FWRP 
        ON (ACFWRP.FUND_WRAPPER_ID=FWRP.FUND_WRAPPER_ID
        AND TO_DATE(NAV_DATE, 'YYYYMMDD')>= FWRP.FROM_DATE
        AND  TO_DATE(NAV_DATE, 'YYYYMMDD')<= FWRP.TO_DATE
        )
),
OVR_RECORDS AS (
    SELECT *
    FROM
        (
        SELECT
             HASH_DIFF
            ,OVERRIDE_TYPE
            ,BUSINESS_DATE
            ,LOAD_DATETIME
            ,ATTRIBUTE_NAME
            ,ATTRIBUTE_VALUE
            ,ROW_NUMBER() OVER (PARTITION BY HASH_DIFF, ATTRIBUTE_NAME
                                  ORDER BY LOAD_DATETIME DESC) AS "RECORD_PRIORITY"
        FROM
            IM_VAULT_DEV.NEEDED_A_PLACE.OVERRIDE_LOOKUP
        )
    WHERE "RECORD_PRIORITY" = '1'
),
EXCLUDE_CHECK AS (
    SELECT
        OVR_R.HASH_DIFF
    FROM
        OVR_RECORDS OVR_R
    WHERE OVR_R.OVERRIDE_TYPE = 'Exclude'
),
PIVOT_OVR AS (
    SELECT
        OVR_R.HASH_DIFF
        ,MAX(CASE
             WHEN OVR_R.ATTRIBUTE_NAME = 'NAV_CALC_DATE'
             AND OVR_R.OVERRIDE_TYPE = 'Override'
             THEN OVR_R.ATTRIBUTE_VALUE END) AS OVERRIDE_NAV_CALC_DATE
        ,MAX(CASE
             WHEN OVR_R.ATTRIBUTE_NAME = 'NET_ASSET_VALUE'
             AND OVR_R.OVERRIDE_TYPE = 'Override'
             THEN OVR_R.ATTRIBUTE_VALUE END) AS OVERRIDE_NET_ASSET_VALUE
        ,MAX(CASE
             WHEN OVR_R.ATTRIBUTE_NAME = 'NAV_PER_SHARE'
             AND OVR_R.OVERRIDE_TYPE = 'Override'
             THEN OVR_R.ATTRIBUTE_VALUE END) AS OVERRIDE_NAV_PER_SHARE
        ,MAX(CASE
             WHEN OVR_R.ATTRIBUTE_NAME = 'NET_CAPSTOCK_SHARES_OUTSTANDING'
             AND OVR_R.OVERRIDE_TYPE = 'Override'
             THEN OVR_R.ATTRIBUTE_VALUE END) AS OVERRIDE_NET_CAPSTOCK_SHARES_OUTSTANDING
        ,MAX(OVR_R.LOAD_DATETIME) AS "LOAD_DATETIME"
    FROM
        OVR_RECORDS OVR_R
    WHERE OVR_R.OVERRIDE_TYPE = 'Override'
    GROUP BY OVR_R.HASH_DIFF
)
SELECT
    FNS.FUND_WRAPPER_ID AS FUND_WRAPPER_ID
    ,FNS.FUND_WRAPPER_HKEY AS FUND_WRAPPER_HKEY
    ,FNS.BUSINESS_DATE AS BUSINESS_DATE
    ,COALESCE(CASE
              WHEN OL."OVERRIDE_NAV_CALC_DATE" IS NOT NULL
              AND OL.LOAD_DATETIME > FNS.LOAD_DATETIME
              THEN OL."OVERRIDE_NAV_CALC_DATE"
              ELSE NULL
              END, FNS.NAV_CALC_DATE) AS "NAV_CALC_DATE"
    ,COALESCE(CASE
              WHEN OL."OVERRIDE_NET_ASSET_VALUE" IS NOT NULL
              AND OL.LOAD_DATETIME > FNS.LOAD_DATETIME
              THEN OL."OVERRIDE_NET_ASSET_VALUE"
              ELSE NULL
              END, FNS.NET_ASSET_VALUE) AS "NET_ASSET_VALUE"
     ,COALESCE(CASE
              WHEN OL."OVERRIDE_NAV_PER_SHARE" IS NOT NULL
              AND OL.LOAD_DATETIME > FNS.LOAD_DATETIME
              THEN OL."OVERRIDE_NAV_PER_SHARE"
              ELSE NULL
              END, FNS.NAV_PER_SHARE) AS "NAV_PER_SHARE"
     ,COALESCE(CASE
              WHEN OL."OVERRIDE_NET_CAPSTOCK_SHARES_OUTSTANDING" IS NOT NULL
              AND OL.LOAD_DATETIME > FNS.LOAD_DATETIME
              THEN OL."OVERRIDE_NET_CAPSTOCK_SHARES_OUTSTANDING"
              ELSE NULL
              END, FNS.NET_CAPSTOCK_SHARES_OUTSTANDING) AS "NET_CAPSTOCK_SHARES_OUTSTANDING"
    ,FNS.LOAD_DATETIME
    ,FNS.HASH_DIFF
FROM FUND_WRAPPER_NAV_SAT FNS
LEFT JOIN
    PIVOT_OVR OL
    ON OL.HASH_DIFF = FNS.HASH_DIFF
WHERE
    FNS.HASH_DIFF NOT IN (
        SELECT
            HASH_DIFF
        FROM
            EXCLUDE_CHECK
    )
);