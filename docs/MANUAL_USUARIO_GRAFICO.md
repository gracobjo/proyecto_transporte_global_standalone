# Manual de Usuario Grafico — SIMLOG Espana

Guia practica para usar el dashboard sin entrar en detalle de codigo.

## 1) Barra superior

- **Buscador semantico rapido**: escribe terminos como `swagger`, `informes`, `hive`, `rutas`, `faq`.
- Pulsa el hallazgo `Ir a ...` para abrir directamente la seccion funcional.
- **Cabecera**: logo + nombre del proyecto + acceso rapido por pestañas.

## 2) Sidebar

- Muestra el **estado del stack** (HDFS, Kafka, Cassandra, Spark, HiveServer2, Airflow, Swagger API, NiFi).
- Incluye resumen de puertos y accesos a interfaces web.
- Desde esta zona puedes refrescar estado y lanzar acciones globales de arranque.

## 3) Cuadro de mando (consultas e informes)

### 3.1 Consultas supervisadas — Cassandra

- Ejecuta consultas predefinidas seguras.
- Bloque `CQL (copiar / pegar / ejecutar)` permite ejecutar `SELECT` manual.

### 3.2 Consultas supervisadas — Hive

- Ejecuta consultas historicas en HiveServer2.
- Bloque `SQL (copiar / pegar / ejecutar)` permite `SHOW`, `SELECT`, `WITH`, `DESCRIBE`.

### 3.3 Informes a medida (plantillas + PDF)

Permite construir informes personalizados:

1. Elige **plantilla**.
2. Elige **BD** (Cassandra o Hive).
3. Elige **tabla** y **campos** (o modo `SELECT *` para exploracion).
4. Define filtro (`WHERE`), orden (`ORDER BY`) y limite.
5. Pulsa **Previsualizar informe**.
6. Descarga **PDF**.

Incluye:
- plantillas base (`operativa`, `ejecutiva`, `auditoria`),
- plantillas personalizadas guardables,
- aliases de columnas por plantilla (JSON),
- opcion sin `LIMIT` (solo para tablas pequenas).

## 4) Servicios

En la pestaña **Servicios** puedes:
- iniciar,
- comprobar,
- parar

cada componente del stack, incluido **Swagger API (FastAPI)** y **FAQ IA API**.

En cada bloque se muestra:
- estado,
- detalle,
- enlace a interfaz web/cadena de acceso.

### 4.1 FAQ IA

Al final de la pestaña **Servicios** tienes un panel de preguntas frecuentes:

1. Escribe una duda operativa.
2. Pulsa **Preguntar al FAQ IA**.
3. Revisa respuesta, confianza, sugerencias y fuentes.

## 5) Flujo recomendado de uso

1. Verifica servicios activos.
2. Ejecuta ingesta (o lanza DAG en Airflow).
3. Ejecuta procesamiento Spark.
4. Consulta operativo en Cassandra.
5. Consulta historico en Hive.
6. Genera informe PDF para compartir/imprimir.
7. Si hay dudas de uso, consulta **FAQ IA** antes de salir del dashboard.

## 6) Errores frecuentes y lectura rapida

- `No hay keyspaces disponibles en Cassandra`: revisa entorno Python/driver y conexion Cassandra.
- `Solo se permiten ...`: el modo seguro bloquea sentencias fuera de lectura.
- Warning Kafka por timeout: puede ser latencia del comando CLI; refresca estado tras unos segundos.
- Consulta correcta pero sin filas: la consulta es valida, pero no hay datos para ese filtro.

