create or replace view IM_VAULT_DEV.NEEDED_A_PLACE.CR_OVR_SLEEVE_NAV_SAT(
	SLEEVE_ID,
	SLEEVE_HKEY,
	BUSINESS_DATE,
	NAV_CALC_DATE,
	NET_ASSET_VALUE,
	NAV_PER_SHARE,
	NET_CAPSTOCK_SHARES_OUTSTANDING,
	LOAD_DATETIME
) as
(  
    WITH ACSLV AS(
  SELECT * 
, TRY_TO_DATE(BUS_DATE::VARCHAR,'YYYYMMDD') AS FROM_DATE
, IFNULL(TRY_TO_DATE(LAG(BUS_DATE) OVER (PARTITION BY  SLEEVE_INSIGHTS_TICKER
         ORDER BY TRY_TO_DATE(BUS_DATE::VARCHAR,'YYYYMMDD') DESC)::VARCHAR,'YYYYMMDD')-1, TO_DATE('99991231', 'YYYYMMDD')) AS TO_DATE_T
,COALESCE(NULLIF(SLEEVE_SSGS_ACCOUNT_CLOSED_DATE, ''), TO_DATE_T) TO_DATE

FROM IM_VAULT.RDV.ACCOUNT_SLEEVE_SAT
    --   QUALIFY ROW_NUMBER() OVER(PARTITION BY SLEEVE_ID ORDER BY FROM_DATE DESC)=1
 ORDER BY LNK_ACCOUNT_SLEEVE_BKEY
), 
SLV AS (
  SELECT * 
, TRY_TO_DATE(BUS_DATE::VARCHAR,'YYYYMMDD') AS FROM_DATE
, IFNULL(TRY_TO_DATE(LAG(BUS_DATE) OVER (PARTITION BY SLEEVE_HKEY 
         ORDER BY TRY_TO_DATE(BUS_DATE::VARCHAR,'YYYYMMDD') DESC)::VARCHAR,'YYYYMMDD')-1, TO_DATE('99991231', 'YYYYMMDD')) AS TO_DATE_T 
,COALESCE(NULLIF(SLEEVE_CLOSED_DATE, ''), TO_DATE_T) TO_DATE
  FROM IM_VAULT.RDV.SLEEVE_SAT ORDER BY SLEEVE_BKEY
),
SLEEVE_NAV_SAT AS (
    SELECT DISTINCT
        SLV.SLEEVE_ID AS SLEEVE_ID,
        SLV.SLEEVE_HKEY AS SLEEVE_HKEY,
        TRY_TO_DATE(NAV.BUSINESS_DATE,'YYYY-MM-DD') AS BUSINESS_DATE,
        TRY_TO_DATE(NAV.NAV_DATE,'YYYYMMDD') AS NAV_CALC_DATE,
        TRY_TO_NUMERIC(NAV.NET_ASSET, 38, 2) AS NET_ASSET_VALUE,
        -- NV.NAV_CHG AS NAV_CHG,
        TO_NUMERIC(NAV.NAV, 38, 4) AS NAV_PER_SHARE,
        TO_NUMERIC(NAV.NET_SHARES_OUTST, 38, 3) AS NET_CAPSTOCK_SHARES_OUTSTANDING,
        NAV.HASH_DIFF,
        NAV.LOAD_DATETIME
    FROM 
      IM_VAULT.RDV.NAV_SSB_SAT NAV   
    JOIN ACSLV ON ACSLV.STT_MCH_ACCOUNT_CODE=NAV.FUND_ID AND ACSLV.STT_MCH_BASIS_IND=NAV.BASIS_IND
    AND TO_DATE(NAV_DATE,'YYYYMMDD') BETWEEN ACSLV.FROM_DATE AND ACSLV.TO_DATE 
    
    JOIN SLV ON SLV.SLEEVE_ID=ACSLV.SLEEVE_ID
    AND TO_DATE(NAV_DATE,'YYYYMMDD') BETWEEN SLV.FROM_DATE AND SLV.TO_DATE
),
OVR_RECORDS AS (
    SELECT
        HASH_DIFF
        ,OVERRIDE_TYPE
        ,BUSINESS_DATE
        ,LOAD_DATETIME
        ,ATTRIBUTE_NAME
        ,ATTRIBUTE_VALUE
        ,TABLE_NAME
    FROM
        IM_VAULT_DEV.NEEDED_A_PLACE.OVERRIDE_LOOKUP
)
SELECT
    SNS.SLEEVE_ID AS SLEEVE_ID
    ,SNS.SLEEVE_HKEY AS SLEEVE_HKEY
    ,SNS.BUSINESS_DATE AS BUSINESS_DATE
    ,SNS.NAV_CALC_DATE AS NAV_CALC_DATE
    ,SNS.NET_ASSET_VALUE AS NET_ASSET_VALUE
    ,SNS.NAV_PER_SHARE AS NAV_PER_SHARE
    ,SNS.NET_CAPSTOCK_SHARES_OUTSTANDING AS NET_CAPSTOCK_SHARES_OUTSTANDING
    ,SNS.LOAD_DATETIME
    ,SNS.HASH_DIFF
FROM SLEEVE_NAV_SAT SNS
LEFT JOIN
    IM_VAULT_DEV.NEEDED_A_PLACE.OVERRIDE_LOOKUP OL
    ON OL.HASH_DIFF = SNS.HASH_DIFF
    AND OL.OVERRIDE_TYPE = 'Override'
WHERE
    NOT EXISTS(
        SELECT 1
        FROM
            IM_VAULT_DEV.NEEDED_A_PLACE.OVERRIDE_LOOKUP OL2
        WHERE
            OL2.HASH_DIFF = SNS.HASH_DIFF
        AND OL2.OVERRIDE_TYPE = 'Exclude'
    )
);