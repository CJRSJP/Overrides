create or replace view IM_VAULT_DEV.BDV.FUND_CLASS_SAT_CR_DEV(
	FUND_CLASS_HKEY,
	FUND_CLASS_BKEY,
	TO_DATE,
	FROM_DATE,
	FUND_CLASS_NAME,
	SHARE_CLASS,
	UNIT_TYPE,
	CURRENCY_ISO,
	CURRENCY_HEDGED_IND,
	ASIA_CLASS_IND,
	DATE_LAUNCHED,
	DATE_CLOSED,
	SEDOL,
	ISIN,
	MEXID,
	CITICODE,
	LOAD_DATETIME
) COMMENT='NO COMMENT PROVIDED'
 as
(
WITH FC_SAT AS(
  SELECT FUND_CLASS_HKEY
        ,FUND_CLASS_BKEY
        ,TRY_TO_DATE(BUS_DATE::VARCHAR,'DD/MM/YYYY') AS FROM_DATE
        ,TRY_TO_DATE(lag(bus_date) OVER (PARTITION BY fund_class_hkey ORDER BY bus_date desc)::VARCHAR,'DD/MM/YYYY') AS TO_DATE
        ,TO_CHAR(FUND_CLASS_NAME ) AS FUND_CLASS_NAME
        ,TO_CHAR(SHARE_CLASS) AS SHARE_CLASS
        ,TO_CHAR(UNIT_TYPE) AS UNIT_TYPE
        ,TO_CHAR(CURRENCY) AS CURRENCY_ISO
        ,CURRENCY_HEDGED_IND
        ,ASIA_CLASS_IND
        ,TRY_TO_DATE(DATE_LAUNCHED::VARCHAR,'DD/MM/YYYY') AS DATE_LAUNCHED
        ,TRY_TO_DATE(DATE_CLOSED::VARCHAR,'DD/MM/YYYY') AS DATE_CLOSED
        ,TO_CHAR(SEDOL) AS SEDOL
        ,TO_CHAR(ISIN) AS ISIN
        ,TO_CHAR(MEXID) AS MEXID
        ,TO_CHAR(CITICODE) AS CITICODE
        ,TO_DATE(LOAD_DATETIME) AS LOAD_DATETIME
  FROM IM_VAULT.RDV.FUND_CLASS_SAT
)
, OVR_RECORDS AS (
    SELECT *
    FROM (
        SELECT OL.H_KEY,
               OL.BUSINESS_DATE,
               OL.ATTRIBUTE_NAME,
               OL.ATTRIBUTE_VALUE,
               OL.LOAD_DATETIME,
               ROW_NUMBER() OVER (PARTITION BY OL.H_KEY, OL.BUSINESS_DATE, OL.ATTRIBUTE_NAME
                                  ORDER BY OL.LOAD_DATETIME DESC) AS "DECLARED_RANK"
        FROM IM_VAULT_DEV.REFERENCE.OVERRIDE_LOOKUP OL)
    WHERE "DECLARED_RANK" = '1'
)
SELECT FCS.FUND_CLASS_HKEY
       , FCS.FUND_CLASS_BKEY
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'TO_DATE'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.TO_DATE
                 ) AS "TO_DATE"
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'FROM_DATE'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.FROM_DATE
                 ) AS "FROM_DATE"          
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'FUND_CLASS_NAME'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.FUND_CLASS_NAME
                 ) AS "FCS.FUND_CLASS_NAME"
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'SHARE_CLASS'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.SHARE_CLASS
                 ) AS "SHARE_CLASS"
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'UNIT_TYPE'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.UNIT_TYPE
                 ) AS "UNIT_TYPE"
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'CURRENCY_ISO'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.CURRENCY_ISO
                 ) AS "CURRENCY_ISO"
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'CURRENCY_HEDGED_IND'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.CURRENCY_HEDGED_IND
                 ) AS "CURRENCY_HEDGED_IND"
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'ASIA_CLASS_IND'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.ASIA_CLASS_IND
                 ) AS "ASIA_CLASS_IND"
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'DATE_LAUNCHED'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.DATE_LAUNCHED
                 ) AS "DATE_LAUNCHED"
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'DATE_CLOSED'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.DATE_CLOSED
                 ) AS "DATE_CLOSED"
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'SEDOL'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.SEDOL
                 ) AS "SEDOL"
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'ISIN'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.ISIN
                 ) AS "ISIN"
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'MEXID'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.MEXID
                 ) AS "MEXID"
       , COALESCE(MAX(CASE
                      WHEN OL.ATTRIBUTE_NAME = 'CITICODE'
                      THEN OL.ATTRIBUTE_VALUE END), FCS.CITICODE
                 ) AS "CITICODE"
       , FCS.LOAD_DATETIME
FROM FC_SAT FCS
LEFT JOIN OVR_RECORDS OL ON OL.H_KEY = FCS.FUND_CLASS_HKEY AND
                            OL.BUSINESS_DATE = FCS.TO_DATE AND
                            OL.ATTRIBUTE_NAME IN (  'TO_DATE'
                                                  , 'FROM_DATE'
                                                  , 'FUND_CLASS_NAME'
                                                  , 'SHARE_CLASS'
                                                  , 'UNIT_TYPE'
                                                  , 'CURRENCY_ISO'
                                                  , 'CURRENY_HEDGED_IND'
                                                  , 'ASIA_CLASS_IND'
                                                  , 'DATE_LAUNCHED'
                                                  , 'SEDOL'
                                                  , 'ISIN'
                                                  , 'MEXID'
                                                  , 'CITICODE') AND
                                                  OL.LOAD_DATETIME > FCS.LOAD_DATETIME
GROUP BY FCS.FUND_CLASS_HKEY
       , FCS.FUND_CLASS_BKEY
       , FCS.TO_DATE
       , FCS.FROM_DATE
       , FCS.FUND_CLASS_NAME
       , FCS.SHARE_CLASS
       , FCS.UNIT_TYPE
       , FCS.CURRENCY_ISO
       , FCS.CURRENCY_HEDGED_IND
       , FCS.ASIA_CLASS_IND
       , FCS.DATE_LAUNCHED
       , FCS.DATE_CLOSED
       , FCS.SEDOL
       , FCS.ISIN
       , FCS.MEXID
       , FCS.CITICODE
       , FCS.LOAD_DATETIME
);