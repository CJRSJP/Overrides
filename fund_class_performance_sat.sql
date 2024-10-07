WITH OVR_RECORDS AS (
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
SELECT TOP 100
        FUND_CLASS_HKEY,
        FUND_CLASS_BKEY,
        PERF_DATE,
        COALESCE(MAX(CASE
                     WHEN OL.ATTRIBUTE_NAME = 'ABS_PERF_DAILY'
                     THEN OL.ATTRIBUTE_VALUE END), FCSPS.ABS_PERF_DAILY
                ) AS "ABS_PERF_DAILY",
        COALESCE(MAX(CASE
                     WHEN OL.ATTRIBUTE_NAME = 'ABS_PERF_WEEKLY'
                     THEN OL.ATTRIBUTE_VALUE END), FCSPS.ABS_PERF_WEEKLY
                ) AS "ABS_PERF_WEEKLY",
        COALESCE(MAX(CASE
                     WHEN OL.ATTRIBUTE_NAME = 'ABS_PERF_M1'
                     THEN OL.ATTRIBUTE_VALUE END), FCSPS.ABS_PERF_M1
                ) AS "ABS_PERF_M1",
        COALESCE(MAX(CASE
                     WHEN OL.ATTRIBUTE_NAME = 'ABS_PERF_M3'
                     THEN OL.ATTRIBUTE_VALUE END), FCSPS.ABS_PERF_M3
                ) AS "ABS_PERF_M3",
        COALESCE(MAX(CASE
                     WHEN OL.ATTRIBUTE_NAME = 'ABS_PERF_M6'
                     THEN OL.ATTRIBUTE_VALUE END), FCSPS.ABS_PERF_M6
                ) AS "ABS_PERF_M6",
        COALESCE(MAX(CASE
                     WHEN OL.ATTRIBUTE_NAME = 'ABS_PERF_Y1'
                     THEN OL.ATTRIBUTE_VALUE END), FCSPS.ABS_PERF_Y1
                ) AS "ABS_PERF_Y1",
        COALESCE(MAX(CASE
                     WHEN OL.ATTRIBUTE_NAME = 'ABS_PERF_Y3'
                     THEN OL.ATTRIBUTE_VALUE END), FCSPS.ABS_PERF_Y3
                ) AS "ABS_PERF_Y3",
        COALESCE(MAX(CASE
                     WHEN OL.ATTRIBUTE_NAME = 'ABS_PERF_Y5'
                     THEN OL.ATTRIBUTE_VALUE END), FCSPS.ABS_PERF_Y5
                ) AS "ABS_PERF_Y5",
        COALESCE(MAX(CASE
                     WHEN OL.ATTRIBUTE_NAME = 'ABS_PERF_Y7'
                     THEN OL.ATTRIBUTE_VALUE END), FCSPS.ABS_PERF_Y7
                ) AS "ABS_PERF_Y7",
        COALESCE(MAX(CASE
                     WHEN OL.ATTRIBUTE_NAME = 'ABS_PERF_Y10'
                     THEN OL.ATTRIBUTE_VALUE END), FCSPS.ABS_PERF_Y10
                ) AS "ABS_PERF_Y10",
        COALESCE(MAX(CASE
                     WHEN OL.ATTRIBUTE_NAME = 'ABS_PERF_INCEPTION'
                     THEN OL.ATTRIBUTE_VALUE END), FCSPS.ABS_PERF_INCEPTION
                ) AS "ABS_PERF_INCEPTION",
        FCSPS.LOAD_DATETIME
FROM IM_VAULT_DEV.BDV.FUND_CLASS_PERFORMANCE_SAT FCSPS
LEFT JOIN OVR_RECORDS OL ON OL.H_KEY = FCSPS.FUND_CLASS_HKEY AND
                            OL.BUSINESS_DATE = FCSPS.PERF_DATE AND
                            OL.ATTRIBUTE_NAME IN ('ABS_PERF_DAILY',
                                                  'ABS_PERF_WEEKLY',
                                                  'ABS_PERF_M1',
                                                  'ABS_PERF_M3',
                                                  'ABS_PERF_M6',
                                                  'ABS_PERF_Y1',
                                                  'ABS_PERF_Y3',
                                                  'ABS_PERF_Y5',
                                                  'ABS_PERF_Y7',
                                                  'ABS_PERF_Y10',
                                                  'ABS_PERF_INCEPTION') AND
                                                  OL.LOAD_DATETIME > FCSPS.LOAD_DATETIME
WHERE FCSPS.FUND_CLASS_HKEY = '00B26A14246AF2997E897E6C56A03F7C' AND FCSPS.PERF_DATE = '2016-12-27'
GROUP BY FCSPS.FUND_CLASS_HKEY, 
         FCSPS.FUND_CLASS_BKEY, 
         FCSPS.PERF_DATE, 
         FCSPS.ABS_PERF_DAILY,
         FCSPS.ABS_PERF_WEEKLY,
         FCSPS.ABS_PERF_M1,
         FCSPS.ABS_PERF_M3,
         FCSPS.ABS_PERF_M6,
         FCSPS.ABS_PERF_Y1,
         FCSPS.ABS_PERF_Y3,
         FCSPS.ABS_PERF_Y5,
         FCSPS.ABS_PERF_Y7,
         FCSPS.ABS_PERF_Y10,
         FCSPS.ABS_PERF_INCEPTION,
         FCSPS.LOAD_DATETIME
ORDER BY FUND_CLASS_HKEY;