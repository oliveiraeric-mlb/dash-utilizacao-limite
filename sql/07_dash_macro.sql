-- DASH_MACRO: agregados globais por segmento/safra (alimenta KPIs e curvas)
CREATE OR REPLACE TABLE `meli-bi-data.SBOX_CREDITSTC.DASH_MACRO` AS
WITH base AS (
  SELECT
    FORMAT_DATE('%Y-%m', FECHA_CONVERSAO) AS safra,
    CASE WHEN grupo_especial = 'BAU-' OR UPPER(grupo_especial) LIKE '%MTC%'
         THEN 'micro_tc' ELSE 'tc_full' END AS segmento,
    CAST(CCARD_GLOBAL_LIMIT_AMT_LC AS FLOAT64) AS limite,
    SAFE_DIVIDE(CAST(TPV_D0  AS FLOAT64), CCARD_GLOBAL_LIMIT_AMT_LC)*100 AS pct_d0,
    SAFE_DIVIDE(CAST(TPV_D3  AS FLOAT64), CCARD_GLOBAL_LIMIT_AMT_LC)*100 AS pct_d3,
    SAFE_DIVIDE(CAST(TPV_D7  AS FLOAT64), CCARD_GLOBAL_LIMIT_AMT_LC)*100 AS pct_d7,
    SAFE_DIVIDE(CAST(TPV_D14 AS FLOAT64), CCARD_GLOBAL_LIMIT_AMT_LC)*100 AS pct_d14,
    SAFE_DIVIDE(CAST(TPV_D30 AS FLOAT64), CCARD_GLOBAL_LIMIT_AMT_LC)*100 AS pct_d30,
    DEUDA_OVER30_MOB3, DEUDA_MOB3
  FROM `meli-bi-data.SBOX_CREDITSTC.0_TPV_PRIMEIRO_USO_TC_MP_POST_ADQ`
  WHERE FECHA_CONVERSAO >= '2025-04-01'
    AND CCARD_GLOBAL_LIMIT_AMT_LC > 0
)
SELECT
  safra,
  segmento,
  COUNT(*)                                                               AS cl,
  ROUND(AVG(pct_d0),  1)                                                AS d0,
  ROUND(AVG(pct_d3),  1)                                                AS d3,
  ROUND(AVG(pct_d7),  1)                                                AS d7,
  ROUND(AVG(pct_d14), 1)                                                AS d14,
  ROUND(AVG(pct_d30), 1)                                                AS d30,
  ROUND(AVG(IF(pct_d0  >= 80, 1, 0))*100, 1)                           AS u80d0,
  ROUND(AVG(IF(pct_d7  >= 80, 1, 0))*100, 1)                           AS u80d7,
  ROUND(AVG(IF(pct_d30 >= 80, 1, 0))*100, 1)                           AS u80d30,
  ROUND(SAFE_DIVIDE(SUM(DEUDA_OVER30_MOB3), SUM(DEUDA_MOB3))*100, 2)   AS over30
FROM base
GROUP BY ALL
ORDER BY safra, segmento
