-- =============================================================================
-- Dataset de entrenamiento para KNIME — SIMLOG (Hive)
-- Features proxy + etiqueta binaria congestion (0/1)
-- Sustituye transporte_ingesta_completa si SIMLOG_HIVE_TABLA_TRANSPORTE es otro nombre.
-- =============================================================================

USE logistica_espana;

WITH base AS (
  SELECT
    t.hub_actual,
    t.anio,
    t.mes,
    t.dia,
    COALESCE(t.hora, 12) AS hora,
    CAST(t.progreso_pct AS DOUBLE) AS progreso_pct,
    CAST(t.distancia_total_km AS DOUBLE) AS dist_km,
    translate(lower(COALESCE(t.estado_ruta, '')), 'áéíóúñ', 'aeiouun') AS estado_ruta_n,
    translate(lower(COALESCE(t.motivo_retraso, '')), 'áéíóúñ', 'aeiouun') AS motivo_n,
    c.temperatura,
    translate(lower(COALESCE(c.descripcion, '')), 'áéíóúñ', 'aeiouun') AS desc_clima,
    translate(lower(COALESCE(c.estado_carretera, '')), 'áéíóúñ', 'aeiouun') AS estado_carretera_n,
    n.tipo AS tipo_via
  FROM transporte_ingesta_completa t
  LEFT JOIN clima_historico c
    ON c.ciudad = t.hub_actual
   AND c.anio = t.anio AND c.mes = t.mes AND c.dia = t.dia
  LEFT JOIN red_gemelo_nodos n
    ON n.id_nodo = t.hub_actual
  WHERE t.anio_part = year(current_date())
    AND t.mes_part = month(current_date())
    AND t.hub_actual IS NOT NULL
),
ev AS (
  SELECT hub_asociado, anio, mes, dia, COUNT(*) AS cnt
  FROM eventos_historico
  WHERE anio_part = year(current_date()) AND mes_part = month(current_date())
  GROUP BY hub_asociado, anio, mes, dia
)
SELECT
  CAST(
    LEAST(120.0, GREATEST(5.0, 100.0 - LEAST(COALESCE(b.progreso_pct, 0.0), 99.0)))
    AS DOUBLE
  ) AS velocidad_media,
  CAST(COALESCE(ev.cnt, 0) AS DOUBLE) AS densidad_trafico,
  CAST(b.temperatura AS DOUBLE) AS temperatura,
  CASE
    WHEN b.desc_clima RLIKE 'lluvia|rain|chubasco|llovizna|drizzle|precip' THEN 1
    ELSE 0
  END AS lluvia,
  CAST(b.hora AS INT) AS hora,
  COALESCE(b.tipo_via, 'desconocido') AS tipo_via,
  CASE
    WHEN b.estado_ruta_n RLIKE 'retraso|bloque|congest|parado|incid'
      OR b.motivo_n RLIKE 'niebla|lluvia|obra|congest|bloqueo'
      OR (LENGTH(TRIM(b.estado_carretera_n)) > 0 AND b.estado_carretera_n NOT IN ('optimo', 'optimal', 'ok', ''))
    THEN 1
    ELSE 0
  END AS congestion
FROM base b
LEFT JOIN ev
  ON ev.hub_asociado = b.hub_actual
 AND ev.anio = b.anio AND ev.mes = b.mes AND ev.dia = b.dia
;
