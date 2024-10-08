create or replace view IM_VAULT.BDV.FUND_CLASS_VOLATILITY_SAT(
	FUND_CLASS_HKEY,
	FUND_CLASS_BKEY,
	VOLALTILITY_DATE,
	VOLATILITY_1Y,
	VOLATILITY_3Y,
	VOLATILITY_5Y,
	VOLATILITY_10Y,
	VOLATILITY_INCEPTION,
	LOAD_DATETIME
) COMMENT='NO COMMENT PROVIDED'
 as
(
WITH PERF_START AS(
SELECT FUND_CLASS_HKEY, MIN(PRICE_DATE) AS PERF_START_DATE
FROM "IM_VAULT".BDV."FUND_CLASS_PRICE_SAT"
GROUP BY FUND_CLASS_HKEY
)
, FC_VOLATILITY AS(
SELECT FVPS.FUND_CLASS_HKEY,FVPS.FUND_CLASS_BKEY, FVPS.PERF_DATE AS VOLALTILITY_DATE
,CASE WHEN DATEDIFF(W,PS.PERF_START_DATE, FVPS.PERF_DATE) >=52
    THEN  (SELECT STDDEV(A.ABS_PERF_WEEKLY)
           FROM "IM_VAULT".BDV."FUND_CLASS_PERFORMANCE_SAT" A
           WHERE FVPS.FUND_CLASS_HKEY = A.FUND_CLASS_HKEY
           AND DAYOFWEEK(A.PERF_DATE) = DAYOFWEEK(FVPS.PERF_DATE)
           AND A.PERF_DATE <= FVPS.PERF_DATE
           AND A.PERF_DATE > DATEADD(W,-52,FVPS.PERF_DATE)
          )
     ELSE NULL
END * SQRT(52) AS VOLATILITY_1Y
,CASE WHEN DATEDIFF(W,PS.PERF_START_DATE, FVPS.PERF_DATE) >=52
    THEN  (SELECT STDDEV(A.ABS_PERF_WEEKLY)
           FROM "IM_VAULT".BDV."FUND_CLASS_PERFORMANCE_SAT" A
           WHERE FVPS.FUND_CLASS_HKEY = A.FUND_CLASS_HKEY
           AND DAYOFWEEK(A.PERF_DATE) = DAYOFWEEK(FVPS.PERF_DATE)
           AND A.PERF_DATE <= FVPS.PERF_DATE
           AND A.PERF_DATE > DATEADD(W,-156,FVPS.PERF_DATE)
          )
     ELSE NULL
END * SQRT(52) AS VOLATILITY_3Y
,CASE WHEN DATEDIFF(W,PS.PERF_START_DATE, FVPS.PERF_DATE) >=52
    THEN  (SELECT STDDEV(A.ABS_PERF_WEEKLY)
           FROM "IM_VAULT".BDV."FUND_CLASS_PERFORMANCE_SAT" A
           WHERE FVPS.FUND_CLASS_HKEY = A.FUND_CLASS_HKEY
           AND DAYOFWEEK(A.PERF_DATE) = DAYOFWEEK(FVPS.PERF_DATE)
           AND A.PERF_DATE <= FVPS.PERF_DATE
           AND A.PERF_DATE > DATEADD(W,-260,FVPS.PERF_DATE)
          )
     ELSE NULL
END * SQRT(52) AS VOLATILITY_5Y
,CASE WHEN DATEDIFF(W,PS.PERF_START_DATE, FVPS.PERF_DATE) >=52
    THEN  (SELECT STDDEV(A.ABS_PERF_WEEKLY)
           FROM "IM_VAULT".BDV."FUND_CLASS_PERFORMANCE_SAT" A
           WHERE FVPS.FUND_CLASS_HKEY = A.FUND_CLASS_HKEY
           AND DAYOFWEEK(A.PERF_DATE) = DAYOFWEEK(FVPS.PERF_DATE)
           AND A.PERF_DATE <= FVPS.PERF_DATE
           AND A.PERF_DATE > DATEADD(W,-520,FVPS.PERF_DATE)
          )
     ELSE NULL
END * SQRT(52) AS VOLATILITY_10Y
,(SELECT STDDEV(A.ABS_PERF_WEEKLY)
  FROM "IM_VAULT".BDV."FUND_CLASS_PERFORMANCE_SAT" A 
  WHERE A.FUND_CLASS_HKEY = FVPS.FUND_CLASS_HKEY
  AND DAYOFWEEK(A.PERF_DATE) = DAYOFWEEK(FVPS.PERF_DATE)
  AND A.PERF_DATE <= FVPS.PERF_DATE)
--  *SQRT(ABS(DATEDIFF(W,PS.PERF_START_DATE, FVPS.PERF_DATE))) AS VOLATILITY_INCEPTION
  *SQRT(52) AS VOLATILITY_INCEPTION
,FVPS.LOAD_DATETIME
FROM "IM_VAULT".BDV."FUND_CLASS_PERFORMANCE_SAT" FVPS
INNER JOIN PERF_START PS
ON PS.FUND_CLASS_HKEY = FVPS.FUND_CLASS_HKEY
--WHERE FVPS.FUND_CLASS_BKEY ='BKX5CH2'
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