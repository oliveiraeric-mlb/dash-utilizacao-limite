-- DASH_CANAL_CLUSTER: métricas por canal de aquisição × política/safra
CREATE OR REPLACE TABLE `meli-bi-data.SBOX_CREDITSTC.DASH_CANAL_CLUSTER` AS
WITH congrats AS (
  SELECT CUS_CUST_ID, DATA_AQUISICAO,
         IFNULL(FLAG_CANAL_AQUISICAO_SIMP, 'N/D') AS canal
  FROM `meli-bi-data.SBOX_CREDITSTC.0_AUT_TBL_CONGRATS_ADQ_MLB_TOTAL`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CUS_CUST_ID, DATA_AQUISICAO ORDER BY DT_CONGRATS DESC) = 1
),
base AS (
  SELECT
    FORMAT_DATE('%Y-%m', t.FECHA_CONVERSAO) AS safra,
    CASE
      WHEN t.grupo_especial = 'BAU-'                            THEN 'micro_tc'
      WHEN UPPER(t.grupo_especial) LIKE '%MTC%'                 THEN 'micro_tc'
      ELSE 'tc_full'
    END AS segmento,
    CASE
      WHEN t.grupo_especial = 'BAU-'                                                                         THEN 'BAU-'
      WHEN t.grupo_especial LIKE 'BAU-MTC SEGMENTOS PREFERENCIAIS%' OR t.grupo_especial LIKE 'N/A-MTC SEGMENTOS PREFERENCIAIS%' THEN 'BAU-MTC SEGMENTOS PREFERENCIAIS'
      WHEN t.grupo_especial LIKE 'BAU-MTC BAU%'                     OR t.grupo_especial LIKE 'N/A-MTC BAU%' THEN 'BAU-MTC BAU'
      WHEN REGEXP_CONTAINS(t.grupo_especial, r'\b9A\b')                                                      THEN 'BAU-9A SELLERS'
      WHEN REGEXP_CONTAINS(t.grupo_especial, r'\b8A\b')                                                      THEN 'BAU-8A SELLERS'
      WHEN REGEXP_CONTAINS(t.grupo_especial, r'\b5A\b')                                                      THEN 'BAU-5A SELLERS'
      WHEN REGEXP_CONTAINS(t.grupo_especial, r'\b6A\b')                                                      THEN 'BAU-6A SELLERS'
      WHEN t.grupo_especial LIKE 'Mar Aberto RTS%'                                                           THEN 'Mar Aberto RTS'
      WHEN t.grupo_especial LIKE 'Mar Aberto Async%'                                                         THEN 'Mar Aberto Async'
      WHEN t.grupo_especial LIKE 'BAU-BAU GRUPO%SIN CAMBIOS%'                                               THEN 'BAU-BAU GRUPO 1y2 SIN CAMBIOS'
      WHEN t.grupo_especial LIKE 'BAU-BAU GRUPO%- UP%'                                                      THEN 'BAU-BAU GRUPO 1y2 UP'
      WHEN t.grupo_especial LIKE 'BAU-HEAVY USERS%'              OR t.grupo_especial LIKE 'N/A-HEAVY USERS%' THEN 'BAU-HEAVY USERS'
      WHEN t.grupo_especial LIKE 'BAU-CARD TO CARD%'             OR t.grupo_especial LIKE 'OPENFINANCE-CARD TO CARD%' THEN 'BAU-CARD TO CARD OPF'
      WHEN t.grupo_especial LIKE 'BAU-USUARIOS%VIP%'             OR t.grupo_especial LIKE 'BAU-USUARIOS MARKETPLACE%'
        OR t.grupo_especial LIKE 'N/A-USUARIOS MARKETPLACE VIP%' OR t.grupo_especial LIKE 'N/A-USUARIOS VIP MARKETPLACE%' THEN 'BAU-USUARIOS MP VIP'
      WHEN t.grupo_especial LIKE 'TEST REACH-TEST NO ECOSISTEMATICOS%' OR UPPER(t.grupo_especial) LIKE 'AMPLIACI_N DE RATINGS-TEST NO ECOSISTEMATICOS%' THEN 'TEST REACH-TEST NO ECOSISTEMATICOS'
      WHEN t.grupo_especial LIKE 'TEST REACH-C1 THIN%'                                                      THEN 'TEST REACH-C1 THIN'
      WHEN t.grupo_especial LIKE 'TEST REACH-%B2 REPEATS%'                                                   THEN 'TEST REACH-B2 REPEATS'
      ELSE NULL
    END AS politica,
    IFNULL(c.canal, 'N/D') AS canal,
    SAFE_DIVIDE(t.TPV_D0,  t.CCARD_GLOBAL_LIMIT_AMT_LC)*100 AS pct_d0,
    SAFE_DIVIDE(t.TPV_D7,  t.CCARD_GLOBAL_LIMIT_AMT_LC)*100 AS pct_d7,
    SAFE_DIVIDE(t.TPV_D30, t.CCARD_GLOBAL_LIMIT_AMT_LC)*100 AS pct_d30,
    t.DEUDA_OVER6_MOB1,  t.DEUDA_MOB1,
    t.DEUDA_OVER15_MOB2, t.DEUDA_MOB2,
    t.DEUDA_OVER30_MOB3, t.DEUDA_MOB3
  FROM `meli-bi-data.SBOX_CREDITSTC.0_TPV_PRIMEIRO_USO_TC_MP_POST_ADQ` t
  LEFT JOIN congrats c
    ON t.CUS_CUST_ID = c.CUS_CUST_ID
   AND DATE(t.FECHA_CONVERSAO) = c.DATA_AQUISICAO
  WHERE t.FECHA_CONVERSAO >= '2025-04-01'
    AND t.CCARD_GLOBAL_LIMIT_AMT_LC > 0
)
SELECT
  safra, segmento, politica, canal,
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
WHERE politica IS NOT NULL
GROUP BY ALL
ORDER BY safra, segmento, politica, canal
