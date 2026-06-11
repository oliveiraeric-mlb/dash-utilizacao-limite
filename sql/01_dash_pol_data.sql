-- DASH_POL_DATA: métricas por política/safra (atualização diária)
-- Agendar no Data Suite: diário 03:00 BRT
CREATE OR REPLACE TABLE `meli-bi-data.SBOX_CREDITSTC.DASH_POL_DATA` AS
SELECT
  FORMAT_DATE('%Y-%m', FECHA_CONVERSAO) AS safra,
  CASE WHEN grupo_especial = 'BAU-' OR UPPER(grupo_especial) LIKE '%MTC%' THEN 'micro_tc' ELSE 'tc_full' END AS seg,
  CASE
    WHEN grupo_especial = 'BAU-'                                                                         THEN 'BAU-'
    WHEN grupo_especial LIKE 'BAU-MTC SEGMENTOS PREFERENCIAIS%' OR grupo_especial LIKE 'N/A-MTC SEGMENTOS PREFERENCIAIS%' THEN 'BAU-MTC SEGMENTOS PREFERENCIAIS'
    WHEN grupo_especial LIKE 'BAU-MTC BAU%'                     OR grupo_especial LIKE 'N/A-MTC BAU%'   THEN 'BAU-MTC BAU'
    WHEN REGEXP_CONTAINS(grupo_especial, r'\b9A\b')                                                      THEN 'BAU-9A SELLERS'
    WHEN REGEXP_CONTAINS(grupo_especial, r'\b8A\b')                                                      THEN 'BAU-8A SELLERS'
    WHEN REGEXP_CONTAINS(grupo_especial, r'\b5A\b')                                                      THEN 'BAU-5A SELLERS'
    WHEN REGEXP_CONTAINS(grupo_especial, r'\b6A\b')                                                      THEN 'BAU-6A SELLERS'
    WHEN grupo_especial LIKE 'Mar Aberto RTS%'                                                           THEN 'Mar Aberto RTS'
    WHEN grupo_especial LIKE 'Mar Aberto Async%'                                                         THEN 'Mar Aberto Async'
    WHEN grupo_especial LIKE 'BAU-BAU GRUPO%SIN CAMBIOS%'                                               THEN 'BAU-BAU GRUPO 1y2 SIN CAMBIOS'
    WHEN grupo_especial LIKE 'BAU-BAU GRUPO%- UP%'                                                      THEN 'BAU-BAU GRUPO 1y2 UP'
    WHEN grupo_especial LIKE 'BAU-HEAVY USERS%'  OR grupo_especial LIKE 'N/A-HEAVY USERS%'              THEN 'BAU-HEAVY USERS'
    WHEN grupo_especial LIKE 'BAU-CARD TO CARD%' OR grupo_especial LIKE 'OPENFINANCE-CARD TO CARD%'     THEN 'BAU-CARD TO CARD OPF'
    WHEN grupo_especial LIKE 'BAU-USUARIOS%VIP%' OR grupo_especial LIKE 'BAU-USUARIOS MARKETPLACE%'
      OR grupo_especial LIKE 'N/A-USUARIOS MARKETPLACE VIP%' OR grupo_especial LIKE 'N/A-USUARIOS VIP MARKETPLACE%' THEN 'BAU-USUARIOS MP VIP'
    WHEN grupo_especial LIKE 'TEST REACH-TEST NO ECOSISTEMATICOS%' OR UPPER(grupo_especial) LIKE 'AMPLIACI_N DE RATINGS-TEST NO ECOSISTEMATICOS%' THEN 'TEST REACH-TEST NO ECOSISTEMATICOS'
    WHEN grupo_especial LIKE 'TEST REACH-C1 THIN%'                                                      THEN 'TEST REACH-C1 THIN'
    WHEN grupo_especial LIKE 'TEST REACH-%B2 REPEATS%'                                                   THEN 'TEST REACH-B2 REPEATS'
    ELSE NULL
  END AS pol,
  COUNT(*)                                                                             AS cl,
  ROUND(AVG(SAFE_DIVIDE(CAST(TPV_D0  AS FLOAT64), CCARD_GLOBAL_LIMIT_AMT_LC)*100), 1) AS d0,
  ROUND(AVG(SAFE_DIVIDE(CAST(TPV_D3  AS FLOAT64), CCARD_GLOBAL_LIMIT_AMT_LC)*100), 1) AS d3,
  ROUND(AVG(SAFE_DIVIDE(CAST(TPV_D7  AS FLOAT64), CCARD_GLOBAL_LIMIT_AMT_LC)*100), 1) AS d7,
  ROUND(AVG(SAFE_DIVIDE(CAST(TPV_D14 AS FLOAT64), CCARD_GLOBAL_LIMIT_AMT_LC)*100), 1) AS d14,
  ROUND(AVG(SAFE_DIVIDE(CAST(TPV_D30 AS FLOAT64), CCARD_GLOBAL_LIMIT_AMT_LC)*100), 1) AS d30,
  ROUND(AVG(IF(SAFE_DIVIDE(CAST(TPV_D0  AS FLOAT64), CCARD_GLOBAL_LIMIT_AMT_LC) >= 0.8, 1, 0))*100, 1) AS u0,
  ROUND(AVG(IF(SAFE_DIVIDE(CAST(TPV_D7  AS FLOAT64), CCARD_GLOBAL_LIMIT_AMT_LC) >= 0.8, 1, 0))*100, 1) AS u7,
  ROUND(AVG(IF(SAFE_DIVIDE(CAST(TPV_D30 AS FLOAT64), CCARD_GLOBAL_LIMIT_AMT_LC) >= 0.8, 1, 0))*100, 1) AS u30,
  ROUND(SAFE_DIVIDE(SUM(CAST(TPV_PIX_D0  AS FLOAT64)), SUM(CAST(TPV_D0  AS FLOAT64)))*100, 1) AS pix0,
  ROUND(SAFE_DIVIDE(SUM(CAST(TPV_PIX_D30 AS FLOAT64)), SUM(CAST(TPV_D30 AS FLOAT64)))*100, 1) AS pix30,
  ROUND(SAFE_DIVIDE(SUM(DEUDA_OVER6_MOB1),  SUM(DEUDA_MOB1)) *100, 2) AS o6,
  ROUND(SAFE_DIVIDE(SUM(DEUDA_OVER15_MOB2), SUM(DEUDA_MOB2))*100, 2)  AS o15,
  ROUND(SAFE_DIVIDE(SUM(DEUDA_OVER30_MOB3), SUM(DEUDA_MOB3))*100, 2)  AS over30
FROM `meli-bi-data.SBOX_CREDITSTC.0_TPV_PRIMEIRO_USO_TC_MP_POST_ADQ`
WHERE FECHA_CONVERSAO >= '2025-04-01'
  AND CCARD_GLOBAL_LIMIT_AMT_LC > 0
GROUP BY ALL
HAVING pol IS NOT NULL
ORDER BY safra, seg, pol
