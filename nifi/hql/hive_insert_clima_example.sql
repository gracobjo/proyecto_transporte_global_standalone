-- Ejemplo para NiFi PutHiveQL (tras EvaluateJsonPath → atributos hub_nombre, temp, etc.)
-- Requiere que la partición `dia` exista o usar insert dinámico según versión Hive

USE logistica_analytics;

-- Ejemplo estático (sustituir por placeholders NiFi Expression Language si tu versión lo permite)
INSERT INTO TABLE clima_hist PARTITION (dia = '2026-03-15')
VALUES (
  'Madrid',
  18.5,
  'clear sky',
  65,
  CURRENT_TIMESTAMP()
);
