# Memoria del Proyecto SIMLOG Espana

## Portada

- **Proyecto:** SIMLOG Espana (Sistema Integrado de Monitorizacion y Simulacion Logistica)
- **Tipo:** Proyecto de ingenieria informatica aplicado a analitica de datos y sistemas distribuidos
- **Modalidad:** Standalone (despliegue en una maquina) con opcion de orquestacion y contenedores
- **Repositorio:** [gracobjo/proyecto_transporte_global_standalone](https://github.com/gracobjo/proyecto_transporte_global_standalone)
- **Version de memoria:** 1.0

---

## Indice

1. Resumen ejecutivo
2. Introduccion y contexto
3. Objetivos del proyecto
4. Alcance y limitaciones
5. Requisitos funcionales y no funcionales
6. Arquitectura y diseno del sistema
7. Modelo de datos y flujo extremo a extremo
8. Implementacion por modulos
9. Interfaz de usuario (frontend) y experiencia de uso
10. Despliegue y operacion (local, Docker, Codespaces)
11. Orquestacion y automatizacion (Airflow y NiFi)
12. Verificacion, pruebas y observabilidad
13. Resultados obtenidos
14. Riesgos, incidencias y mitigaciones
15. Conclusiones y trabajo futuro
16. Anexos de ejecucion rapida
17. Trazabilidad documental (consolidacion de ficheros .md)

---

## 1. Resumen ejecutivo

SIMLOG Espana es una plataforma de simulacion y monitorizacion logistica basada en stack Apache para ejecutar un ciclo KDD completo: ingesta, preprocesamiento, transformacion con grafos, mineria e interpretacion. El sistema integra fuentes de clima, simulacion de incidencias, geolocalizacion de camiones e incidencias reales de trafico de la DGT en formato DATEX2 para construir una vista operativa y analitica de la red de transporte.

La solucion desacopla etapas con Kafka y HDFS, procesa con Spark/GraphFrames, persiste estado operativo en Cassandra e historico en Hive, y ofrece una interfaz Streamlit para operacion, inspeccion de pipeline, planificacion de rutas y simulacion de escenarios. La plataforma se ha diseniado para ejecucion standalone, simplificando su uso docente y de demostracion sin perder trazabilidad tecnica.

---

## 2. Introduccion y contexto

Los sistemas de transporte requieren reaccion rapida ante incidencias (bloqueos, congestion, meteorologia adversa) y, al mismo tiempo, analitica historica para mejorar decisiones. SIMLOG responde a ese problema combinando:

- capa operativa en tiempo casi real,
- capa historica para consultas analiticas,
- interfaz de control unificada para operacion y presentacion.

El proyecto toma como referencia funcional trabajos previos del mismo ecosistema, adaptados a modo standalone para reducir barreras de despliegue y facilitar su evaluacion academica y demostrativa.

---

## 3. Objetivos del proyecto

### 3.1 Objetivo general

Construir una plataforma integrada capaz de simular, procesar y visualizar el estado de una red logistica nacional mediante un ciclo KDD completo y reproducible.

### 3.2 Objetivos especificos

- Implementar ingesta periodica con clima, incidencias y tracking de camiones.
- Integrar una fuente real de trafico (DGT DATEX2) con prioridad controlada sobre la simulacion.
- Desacoplar transporte de datos con Kafka y backup en HDFS.
- Aplicar transformaciones de grafo y reglas de autosanacion con Spark.
- Calcular criticidad de nodos (PageRank) y rutas alternativas.
- Persistir datos operativos en Cassandra e historicos en Hive.
- Proveer frontend de supervision, consulta y planificacion.
- Habilitar operacion manual y automatizada (scripts, Airflow, NiFi).

---

## 4. Alcance y limitaciones

### 4.1 Alcance incluido

- Simulacion de red logistica en Espana (hubs y nodos secundarios).
- Pipeline KDD de extremo a extremo con persistencia dual.
- Frontend Streamlit con 9 pestanias funcionales.
- Integracion con Docker y guia de uso en Codespaces.
- Documentacion tecnica, de usuario y de desarrollador.

### 4.2 Fuera de alcance o parcial

- Alta disponibilidad multi-nodo en produccion.
- SLO/SLA empresariales con monitoreo externo avanzado.
- Endurecimiento completo de seguridad para entorno productivo.
- Operativa full cloud con autoescalado gestionado.

---

## 5. Requisitos funcionales y no funcionales

### 5.1 Requisitos funcionales principales

- Ejecutar ciclo KDD por fases o de forma completa.
- Consultar estado de red, camiones y criticidad.
- Calcular rutas hibridas y alternativas ante incidencias.
- Realizar consultas supervisadas (Cassandra/Hive).
- Ejecutar consultas de lectura desde frontend (SQL/CQL seguro).
- Construir informes a medida por seleccion de tabla/campos/filtros.
- Exportar informes en PDF y reutilizar plantillas de informe.
- Resolver preguntas frecuentes desde un FAQ IA local integrado en la UI.
- Orquestar ejecuciones periodicas.
- Gestionar servicios del stack desde interfaz.

### 5.2 Requisitos no funcionales principales

- Trazabilidad fase-codigo-datos.
- Reproducibilidad en entorno standalone.
- Modularidad y separacion por responsabilidades.
- Tolerancia a degradacion parcial (Hive opcional en ciertos escenarios).
- Observabilidad operativa basica (checks de servicios y pipeline).
- Navegacion asistida por buscador semantico para reducir tiempo de acceso.
- Seguridad de consulta: bloqueo de operaciones de escritura/borrado desde UI.
- Soporte contextual local: FAQ IA sin dependencia de servicios externos.

---

## 6. Arquitectura y diseno del sistema

La arquitectura se organiza en seis capas:

1. **Ingesta:** scripts Python y/o flujos NiFi.
2. **Mensajeria y backup:** Kafka (`transporte_raw`, `transporte_filtered`) y HDFS.
3. **Procesamiento:** Spark con modelado de grafo y reglas de negocio.
4. **Persistencia:** Cassandra (operativa) + Hive (historico/analitica).
5. **Orquestacion:** Airflow, NiFi y scripts de control.
6. **Presentacion:** Streamlit, vistas cartograficas/topologicas y FAQ IA integrada en `Servicios`.

Decisiones de diseno clave:

- Persistencia dual para separar latencia operativa de analitica historica.
- Pipeline desacoplado para robustez y reprocesado.
- Modo standalone para facilitar despliegue y demo.
- UI unificada que combina operacion tecnica y narrativa KDD.

---

## 7. Modelo de datos y flujo extremo a extremo

### 7.1 Entidades operativas

- **Nodos de red:** estado, coordenadas, tipo.
- **Aristas:** distancia y estado operativo.
- **Camiones:** posicion, ruta, origen/destino, estado.
- **Metrica de criticidad:** PageRank por nodo.
- **Contexto de clima:** condiciones por hub.

### 7.2 Flujo de datos

`Ingesta -> Kafka + HDFS -> Spark -> Cassandra + Hive -> Dashboard`

### 7.3 Contrato de datos

El proyecto mantiene contrato canonico para camiones (por ejemplo `id_camion`, `lat`, `lon`, `ruta_origen`, `ruta_destino`) y estructura de estados de nodos/aristas, con enfoque de compatibilidad para consumidores legacy cuando aplica. Tras la integracion DATEX2, el contrato tambien incluye `incidencias_dgt`, `resumen_dgt`, `source`, `severity`, `peso_pagerank` e identificadores de incidencia.

---

## 8. Implementacion por modulos

### 8.1 Ingesta

- Consulta clima por API.
- Simulacion de incidencias y GPS.
- Integracion DATEX2 DGT con cache local y modo degradado.
- Publicacion Kafka y copia HDFS.
- Escritura de snapshot local para consumo de UI y fases Spark.

### 8.2 Procesamiento

- Construccion de grafo desde topologia y estado.
- Reponderacion de criticidad usando `peso_pagerank` cuando la fuente es la DGT.
- Regla de autosanacion:
  - bloqueo: exclusion de arista,
  - congestion/clima adverso: penalizacion de peso.
- Analisis de criticidad (PageRank) y rutas.

### 8.3 Persistencia

- **Cassandra:** tablas operativas para dashboard, ahora con procedencia y severidad en `nodos_estado` y `pagerank_nodos`.
- **Hive:** historico y consultas analiticas orientadas a reporting.

### 8.4 Servicios y utilidades

- Scripts de arranque/parada/comprobacion.
- API y utilidades para consultas supervisadas.
- FAQ IA local con base de conocimiento JSON y API Swagger.
- Componentes de soporte para gemelo digital y asistentes.

---

## 9. Interfaz de usuario (frontend) y experiencia de uso

La aplicacion Streamlit estructura el trabajo en nueve pestanias:

1. **Ciclo KDD:** navegacion por fases, ejecucion por fase y trazabilidad.
2. **Resultados pipeline:** verificaciones por etapa (ingesta, Kafka/HDFS, Spark, Cassandra, Hive).
3. **Cuadro de mando:** operaciones, consultas y slides de clima/retrasos.
4. **Asistente flota:** lenguaje natural hacia consultas supervisadas.
5. **Rutas hibridas:** planificacion con incidencias y alternativas.
6. **Gemelo digital:** simulacion de escenarios y comparacion de rutas.
7. **Servicios:** iniciar/comprobar/parar componentes del stack + panel FAQ IA.
8. **Mapa y metricas:** vista operativa y vista de planificacion.
9. **Verificacion tecnica:** checks rapidos de conectividad y datos.

Aspectos UX destacados:

- selector de fase con navegacion consistente,
- separacion entre topologia logica y mapa geografico,
- trazabilidad visual de cambios de payload,
- toggle de simulacion de incidencias desde frontend,
- visibilidad del modo DGT (`live`, `cache`, `disabled`) y de las alertas de bloqueo,
- buscador semantico en cabecera con salto directo de pestañas,
- constructor de informes a medida con modo `SELECT *` o por campos,
- exportacion PDF para consumo de negocio y auditoria,
- FAQ IA con historial, sugerencias y fuentes para reducir friccion operativa.

---

## 10. Despliegue y operacion (local, Docker, Codespaces)

### 10.1 Entorno local

- venv Python,
- servicios del stack segun disponibilidad,
- ejecucion manual de ingesta/procesamiento/dashboard.
- script dedicado `scripts/ejecutar_ingesta_dgt.py` para probar la rama real.

### 10.2 Docker

El proyecto dispone de guias para stack completo y mapeo practico con el enunciado academico. Permite demostraciones con servicios integrados en contenedores.

### 10.3 GitHub Codespaces

Se incorpora modo demo de bajo riesgo con script de arranque en un comando (`scripts/demo_codespaces.sh`), pensado para presentaciones sin depender de levantar todo el stack pesado.

Adicionalmente, se define un perfil de cluster docente aislado para Codespaces:

- `docker-compose.codespaces.yml`
- `Dockerfile.codespaces`
- `hadoop.codespaces.env`
- guia operativa: `docs/CODESPACES_CLUSTER.md`

Este perfil evita conflictos con el `docker-compose.yml` principal y permite mostrar infraestructura Hadoop/Spark/Kafka/Jupyter en entorno cloud controlado.

### 10.4 Procedimiento resumido del cluster en Codespaces

1. Crear Codespace sobre `main`.
2. Levantar cluster:

```bash
docker compose -f docker-compose.codespaces.yml up -d --build
docker compose -f docker-compose.codespaces.yml ps
```

3. Publicar puertos en modo `Public`: `9870`, `8080`, `8888`.
4. Validar UIs y logs (`namenode`, `spark-master`, `kafka`).
5. Parar/limpiar cuando termine la sesion:

```bash
docker compose -f docker-compose.codespaces.yml down
# o limpieza completa
docker compose -f docker-compose.codespaces.yml down -v
```

---

## 11. Orquestacion y automatizacion (Airflow y NiFi)

### 11.1 Airflow

- DAG maestro y DAGs por fases KDD.
- ejecucion periodica y/o por cadena de fases.
- recomendaciones de configuracion para evitar colas bloqueadas.

### 11.2 NiFi

- documentacion de flujos y procesadores,
- uso de parameter context para variables sensibles,
- soporte de ingesta de clima y persistencia en pipeline.

La coexistencia Airflow/NiFi requiere coordinacion de ventanas para evitar solapamientos.

---

## 12. Verificacion, pruebas y observabilidad

Mecanismos incorporados:

- comprobaciones de servicios por puerto/estado,
- validacion de artefactos de ingesta local,
- verificacion de topics Kafka y backup HDFS,
- lectura de tablas de Cassandra,
- consultas supervisadas en Hive/Cassandra,
- paneles de verificacion en frontend.
- FAQ IA para troubleshooting rapido y autoservicio documental.

Estrategia recomendada:

1. comprobar servicios base,
2. ejecutar ingesta,
3. ejecutar procesamiento,
4. validar resultados en dashboard y consultas.

---

## 13. Resultados obtenidos

- Pipeline KDD funcional y trazable.
- Interfaz unificada para operacion y explicacion del sistema.
- Persistencia operativa e historica separadas por objetivo.
- Capacidad de simulacion de escenarios y rutas alternativas.
- Soporte contextual integrado mediante FAQ IA local y documentada.
- Documentacion extensa para perfiles tecnico, usuario y presentacion.

El estado actual permite demostraciones completas y ejecucion incremental segun recursos disponibles.

---

## 14. Riesgos, incidencias y mitigaciones

### Riesgos identificados

- arranque lento o inestabilidad en servicios pesados,
- dependencias externas (API clima, conectividad),
- diferencias entre entornos (Windows/Linux/Codespaces),
- complejidad de configuracion multi-servicio.

### Mitigaciones aplicadas

- modo standalone y scripts de operacion,
- fallback en componentes opcionales (por ejemplo Hive en ciertas rutas de uso),
- guias de troubleshooting y checklists,
- modo demo en Codespaces para presentacion estable.

---

## 15. Conclusiones y trabajo futuro

SIMLOG Espana consolida en una sola solucion los elementos fundamentales de un proyecto moderno de datos para logistica: ingesta, procesamiento con grafos, persistencia dual, orquestacion e interfaz operativa. El resultado es util tanto para evaluacion academica como para demostraciones tecnicas.

Lineas de evolucion propuestas:

- endurecimiento de seguridad y gestion de secretos para produccion,
- observabilidad avanzada (metricas, tracing, alertas),
- escalado multi-nodo y despliegue cloud nativo,
- ampliacion de modelos de prediccion y deteccion de anomalias,
- empaquetado de release "demo enterprise" con datos semilla.

---

## 16. Anexos de ejecucion rapida

### 16.1 Arranque demo Codespaces

```bash
bash scripts/demo_codespaces.sh
```

Variantes utiles:

```bash
# Solo UI (sin Docker)
SIMLOG_DEMO_DOCKER=0 bash scripts/demo_codespaces.sh

# Con HDFS + Cassandra + Kafka
SIMLOG_DEMO_DOCKER_SERVICES="namenode datanode cassandra kafka" bash scripts/demo_codespaces.sh
```

### 16.2 Arranque local basico

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m ingesta.ingesta_kdd
streamlit run app_visualizacion.py
```

### 16.3 Stack por script

```bash
python -u scripts/simlog_stack.py start
python -u scripts/simlog_stack.py status
python -u scripts/simlog_stack.py stop
```

### 16.4 Cluster aislado en Codespaces

```bash
docker compose -f docker-compose.codespaces.yml up -d --build
docker compose -f docker-compose.codespaces.yml ps
docker compose -f docker-compose.codespaces.yml down
```

---

## 17. Trazabilidad documental (consolidacion de ficheros .md)

Esta memoria sintetiza y agrupa el contenido distribuido en la documentacion existente del repositorio, especialmente:

- `README.md`
- `README_SIMLOG.md`
- `docs/README.md`
- `docs/DISENO_SISTEMA.md`
- `docs/DISENO_ARQUITECTURA.md`
- `docs/CASOS_DE_USO.md`
- `docs/FLUJO_DATOS_Y_REQUISITOS.md`
- `docs/REQUIREMENTS_CHECKLIST.md`
- `docs/DASHBOARD_KDD_UI.md`
- `docs/AIRFLOW.md`
- `docs/YARN_Y_SPARK.md`
- `docs/DIAGRAMAS_MERMAID.md`
- `docs/MANUAL_USUARIO.md`
- `docs/MANUAL_DESARROLLADOR.md`
- `docs/MANUAL_USUARIO_GRAFICO.md`
- `docs/CODESPACES_DEMO.md`
- `docs/CODESPACES_CLUSTER.md`
- `DOCKER.md`
- `GUIA_PRACTICA_DOCKER.md`
- `nifi/README_NIFI.md`
- `nifi/flow/FLUJO_MINIMO_CLIMA.md`
- `nifi/PROCESADORES_Y_RELACIONES.md`
- `orquestacion/README_DAGS_KDD.md`
- `servicios/README_SERVICIOS_API.md`
- `servicios/README_GESTION_SERVICIOS.md`

Con ello se dispone de un documento unico, imprimible y orientado a presentacion formal del proyecto.
