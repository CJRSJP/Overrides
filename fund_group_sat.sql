create or replace view IM_VAULT_DEV.BDV.FUND_GROUP_SAT_CR_DEV(
	FUND_GROUP_HKEY,
	FROM_DATE,
	TO_DATE,
	FUND_GROUP_NAME,
	FUND_GROUP_NAME_SHORT,
	SJP_FUND_ID,
	LOAD_DATETIME
) COMMENT='No Comment Provided'
 as
(
   -- Please type View Definition here
 WITH FGSAT AS (SELECT FGSAT.*
               FROM "IM_VAULT"."RDV"."FUND_GROUP_SAT" FGSAT
               INNER JOIN (SELECT FUND_GROUP_HKEY, BUS_DATE, LAST_VALUE(LOAD_DATETIME) 
                           OVER (PARTITION BY FUND_GROUP_HKEY, BUS_DATE ORDER BY LOAD_DATETIME) AS LOAD_DATETIME
               FROM "IM_VAULT"."RDV"."FUND_GROUP_SAT") A
               ON FGSAT.FUND_GROUP_HKEY = A.FUND_GROUP_HKEY
               AND FGSAT.BUS_DATE = A.BUS_DATE
               AND FGSAT.LOAD_DATETIME = A.LOAD_DATETIME
)
, FUND_GROUP AS (               
SELECT FGSAT.FUND_GROUP_HKEY
      ,TRY_TO_DATE(FGSAT.BUS_DATE::VARCHAR,'DD/MM/YYYY') AS FROM_DATE
      ,IFNULL(TRY_TO_DATE(lag(bus_date) OVER (PARTITION BY FGSAT.FUND_GROUP_HKEY ORDER BY FGSAT.BUS_DATE DESC)::VARCHAR,'DD/MM/YYYY'),'9999-12-31') AS TO_DATE
      ,CAST(FGSAT.FUND_GROUP_NAME AS VARCHAR(500)) AS FUND_GROUP_NAME
      ,CAST(FGSAT.FUND_GROUP_NAME_SHORT AS VARCHAR(500)) AS FUND_GROUP_NAME_SHORT
      ,TRY_TO_BOOLEAN(FGSAT.SJP_FUND_IND) AS SJP_FUND_IND
      ,FGSAT.LOAD_DATETIME
FROM FGSAT
)
, OVR_RECORDS AS (
    SELECT *
    FROM (
        SELECT TRIM(OL.H_KEY) AS "H_KEY",
               TRIM(OL.BUSINESS_DATE) AS "BUSINESS_DATE",
               TRIM(OL.ATTRIBUTE_NAME) AS "ATTRIBUTE_NAME",
               OL.ATTRIBUTE_VALUE,
               TRIM(OL.LOAD_DATETIME) AS "LOAD_DATETIME",
               ROW_NUMBER() OVER (PARTITION BY OL.H_KEY, OL.BUSINESS_DATE, OL.ATTRIBUTE_NAME
                                  ORDER BY OL.LOAD_DATETIME DESC) AS "DECLARED_RANK"
        FROM IM_VAULT_DEV.REFERENCE.OVERRIDE_LOOKUP OL)
    WHERE "DECLARED_RANK" = '1'
)
SELECT  FG.FUND_GROUP_HKEY
        , COALESCE(MAX(CASE
                       WHEN OL.ATTRIBUTE_NAME = 'FROM_DATE'
                       THEN OL.ATTRIBUTE_VALUE END), FG.FROM_DATE
                  ) AS "FROM_DATE"
        , COALESCE(MAX(CASE
                       WHEN OL.ATTRIBUTE_NAME = 'TO_DATE'
                       THEN OL.ATTRIBUTE_VALUE END), FG.TO_DATE
                  ) AS "TO_DATE"
        , COALESCE(MAX(CASE
                       WHEN OL.ATTRIBUTE_NAME = 'FUND_GROUP_NAME'
                       THEN OL.ATTRIBUTE_VALUE END), FG.FUND_GROUP_NAME
                  ) AS "FUND_GROUP_NAME"
        , COALESCE(MAX(CASE
                       WHEN OL.ATTRIBUTE_NAME = 'FUND_GROUP_NAME_SHORT'
                       THEN OL.ATTRIBUTE_VALUE END), FG.FUND_GROUP_NAME_SHORT
                  ) AS "FUND_GROUP_NAME_SHORT"
        , COALESCE(MAX(CASE
                       WHEN OL.ATTRIBUTE_NAME = 'SJP_FUND_ID'
                       THEN OL.ATTRIBUTE_VALUE END), FG.SJP_FUND_IND
                  ) AS "SJP_FUND_ID"
        , FG.LOAD_DATETIME
FROM FUND_GROUP FG
LEFT JOIN OVR_RECORDS OL ON OL.H_KEY = FG.FUND_GROUP_HKEY AND
                            (OL.BUSINESS_DATE = FG.TO_DATE OR OL.BUSINESS_DATE = FG.FROM_DATE) AND
                            OL.ATTRIBUTE_NAME IN ('FROM_DATE'
                                                  , 'TO_DATE'
                                                  , 'FUND_GROUP_NAME'
                                                  , 'FUND_GROUP_NAME_SHORT'
                                                  , 'SJP_FUND_ID') AND
                                                  OL.LOAD_DATETIME > FG.LOAD_DATETIME
GROUP BY FG.FUND_GROUP_HKEY
         , FG.FROM_DATE
         , FG.TO_DATE
         , FG.FUND_GROUP_NAME
         , FG.FUND_GROUP_NAME_SHORT
         , FG.SJP_FUND_IND
         , FG.LOAD_DATETIME
);