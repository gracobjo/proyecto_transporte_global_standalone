# Demo en GitHub Codespaces

Guia rapida para preparar una presentacion estable del proyecto sin levantar todo el stack pesado en vivo.

## 1) Crear Codespace

1. Abre el repo en GitHub.
2. Boton **Code** -> **Codespaces** -> **Create codespace on main**.
3. Si puedes elegir, usa una maquina de al menos 4 cores / 8 GB RAM.

## 2) Arranque en un comando

Desde la raiz del repo, ejecuta:

```bash
bash scripts/demo_codespaces.sh
```

Este script hace:

- crea/activa `.venv`
- instala `requirements.txt`
- crea `.env` desde `.env.example` si no existe
- arranca servicios Docker de demo (`cassandra kafka` por defecto, configurable)
- intenta inicializar esquema Cassandra (`cassandra/esquema_logistica.cql`)
- ejecuta una ingesta inicial (si no la desactivas)
- arranca Streamlit en `0.0.0.0:8501`

## 3) Abrir la app

- En Codespaces, publica/abre el puerto `8501`.
- Abre la URL reenviada por GitHub.

## 4) Ajustes utiles de demo

- Para ejecutar en modo solo-UI (sin Docker):

```bash
SIMLOG_DEMO_DOCKER=0 bash scripts/demo_codespaces.sh
```

- Para levantar tambien HDFS de apoyo:

```bash
SIMLOG_DEMO_DOCKER_SERVICES="namenode datanode cassandra kafka" bash scripts/demo_codespaces.sh
```

- Para omitir la ingesta inicial:

```bash
SIMLOG_DEMO_SKIP_INGESTA=1 bash scripts/demo_codespaces.sh
```

- Para usar otro puerto:

```bash
PORT=8502 bash scripts/demo_codespaces.sh
```

## 5) Checklist de presentacion (5 minutos antes)

1. Verifica que Streamlit abre y que carga la pestana **Ciclo KDD**.
2. Ejecuta una ingesta desde UI (fases 1-2).
3. Prueba el checkbox de simulacion de incidencias (activar/desactivar).
4. Comprueba que se actualiza el timeline y no hay errores visibles.
5. Deja abierta la vista principal y la pestana de resultados que vayas a mostrar.

## Notas

- En Codespaces, levantar simultaneamente Hadoop + Hive + NiFi + Cassandra + Spark puede ser pesado y lento.
- Para una demo estable, prioriza UI + flujo KDD + datos de snapshot.
