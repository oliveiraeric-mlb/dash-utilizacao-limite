-- DASH_CANAL_CROSS_DATA: cruzamentos dimensionais por canal de aquisição
CREATE OR REPLACE TABLE `meli-bi-data.SBOX_CREDITSTC.DASH_CANAL_CROSS_DATA` AS
WITH congrats AS (
  SELECT CUS_CUST_ID, DATA_AQUISICAO,
         IFNULL(FLAG_CANAL_AQUISICAO_SIMP, 'N/D') AS canal
  FROM `meli-bi-data.SBOX_CREDITSTC.0_AUT_TBL_CONGRATS_ADQ_MLB_TOTAL`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CUS_CUST_ID, DATA_AQUISICAO ORDER BY DT_CONGRATS DESC) = 1
),
base AS (
  SELECT
    FORMAT_DATE('%Y-%m', t.FECHA_CONVERSAO)                                     AS safra,
    CASE WHEN t.grupo_especial = 'BAU-' OR UPPER(t.grupo_especial) LIKE '%MTC%'
         THEN 'micro_tc' ELSE 'tc_full' END                                     AS segmento,
    IFNULL(c.canal, 'N/D')                                                      AS canal,
    IFNULL(t.RANGE_BUREAU,   'N/D')  AS rb,
    IFNULL(t.RATING_TC,      'S/R')  AS rt,
    IFNULL(t.FLAG_APP_ATIVO, 'N/D')  AS fa,
    IFNULL(t.FLAG_SELLERS,   'N/D')  AS fs,
    IFNULL(t.NISE_TAG,       'N/D')  AS nt,
    SAFE_DIVIDE(t.TPV_D0,  t.CCARD_GLOBAL_LIMIT_AMT_LC)*100 AS pct_d0,
    SAFE_DIVIDE(t.TPV_D7,  t.CCARD_GLOBAL_LIMIT_AMT_LC)*100 AS pct_d7,
    SAFE_DIVIDE(t.TPV_D30, t.CCARD_GLOBAL_LIMIT_AMT_LC)*100 AS pct_d30,
    t.DEUDA_OVER6_MOB1,  t.DEUDA_MOB1,
    t.DEUDA_OVER15_MOB2, t.DEUDA_MOB2,
    t.DEUDA_OVER30_MOB3, t.DEUDA_MOB3
  FROM `meli-bi-data.SBOX_CREDITSTC.0_TPV_PRIMEIRO_USO_TC_MP_POST_ADQ` t
  LEFT JOIN congrats c ON t.CUS_CUST_ID = c.CUS_CUST_ID AND DATE(t.FECHA_CONVERSAO) = c.DATA_AQUISICAO
  WHERE t.FECHA_CONVERSAO >= '2025-04-01' AND t.CCARD_GLOBAL_LIMIT_AMT_LC > 0
)
SELECT
  safra, segmento, canal, dim1, val1, dim2, val2,
  COUNT(*)                                                              AS cl,
  ROUND(AVG(pct_d0),  1)                                               AS d0,
  ROUND(AVG(pct_d7),  1)                                               AS d7,
  ROUND(AVG(pct_d30), 1)                                               AS d30,
  ROUND(AVG(IF(pct_d0  >= 80, 1, 0))*100, 1)                          AS u0,
  ROUND(AVG(IF(pct_d30 >= 80, 1, 0))*100, 1)                          AS u30,
  ROUND(SAFE_DIVIDE(SUM(DEUDA_OVER6_MOB1),  SUM(DEUDA_MOB1)) *100, 2) AS over6,
  ROUND(SAFE_DIVIDE(SUM(DEUDA_OVER15_MOB2), SUM(DEUDA_MOB2))*100, 2)  AS over15,
  ROUND(SAFE_DIVIDE(SUM(DEUDA_OVER30_MOB3), SUM(DEUDA_MOB3))*100, 2)  AS over30
FROM base
CROSS JOIN UNNEST([
  STRUCT('RANGE_BUREAU' AS dim1, rb AS val1, 'NISE_TAG'      AS dim2, nt AS val2),
  STRUCT('RANGE_BUREAU',         rb,          'FLAG_APP_ATIVO',        fa        ),
  STRUCT('RANGE_BUREAU',         rb,          'FLAG_SELLERS',          fs        ),
  STRUCT('RANGE_BUREAU',         rb,          'RATING_TC',             rt        ),
  STRUCT('NISE_TAG',             nt,          'FLAG_APP_ATIVO',        fa        ),
  STRUCT('NISE_TAG',             nt,          'FLAG_SELLERS',          fs        ),
  STRUCT('NISE_TAG',             nt,          'RATING_TC',             rt        ),
  STRUCT('FLAG_APP_ATIVO',       fa,          'FLAG_SELLERS',          fs        ),
  STRUCT('FLAG_APP_ATIVO',       fa,          'RATING_TC',             rt        ),
  STRUCT('FLAG_SELLERS',         fs,          'RATING_TC',             rt        )
]) AS pairs
GROUP BY ALL
ORDER BY safra, segmento, canal, dim1, val1, dim2, val2
