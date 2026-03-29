# Cluster Big Data en Codespaces (perfil separado)

Guia explicativa paso a paso para levantar un cluster Big Data en GitHub Codespaces sin mezclarlo con el despliegue principal del proyecto.

## Objetivo

Levantar en Codespaces un cluster de practicas con Hadoop + Spark + Kafka + Jupyter usando archivos dedicados:

- `docker-compose.codespaces.yml`
- `Dockerfile.codespaces`
- `hadoop.codespaces.env`

Este perfil **no sustituye** `docker-compose.yml` ni `Dockerfile` del stack principal.

## 1) Requisitos previos

1. Repositorio actualizado en `main`.
2. Codespace creado desde GitHub (Code -> Codespaces -> Create codespace on main).
3. Docker disponible dentro del Codespace (`docker` y `docker compose` en PATH).
4. Recomendado: maquina de 4 cores / 8 GB RAM (si es posible).

Comprobacion rapida:

```bash
docker --version
docker compose version
```

## 2) Arranque del cluster

Desde la raiz del repositorio:

```bash
docker compose -f docker-compose.codespaces.yml up -d --build
docker compose -f docker-compose.codespaces.yml ps
```

Si todo va bien, deberias ver en estado `Up` al menos:

- `cs-namenode`
- `cs-datanode`
- `cs-spark-master`
- `cs-spark-worker`
- `cs-kafka`
- `cs-python-env`

## 3) Publicar puertos en Codespaces

En la pestana **Ports** (parte inferior de VS Code web), pon en **Public**:

- `9870` (Hadoop NameNode UI)
- `8080` (Spark Master UI)
- `8888` (Jupyter)

Opcionales (no web):

- `9092` (Kafka broker)
- `9000` (HDFS RPC)
- `7077` (Spark endpoint)

## 4) Validacion funcional (checklist)

### 4.1 Estado general

```bash
docker compose -f docker-compose.codespaces.yml ps
```

### 4.2 Logs de arranque

```bash
docker compose -f docker-compose.codespaces.yml logs --tail=120 namenode
docker compose -f docker-compose.codespaces.yml logs --tail=120 spark-master
docker compose -f docker-compose.codespaces.yml logs --tail=120 kafka
```

### 4.3 UIs

- Hadoop UI (`9870`) responde.
- Spark UI (`8080`) responde.
- Jupyter (`8888`) responde (token visible en logs de `python-env`).

## 5) Operacion diaria

Parar temporalmente:

```bash
docker compose -f docker-compose.codespaces.yml stop
```

Reanudar:

```bash
docker compose -f docker-compose.codespaces.yml start
```

Parar y borrar contenedores:

```bash
docker compose -f docker-compose.codespaces.yml down
```

Parar y borrar tambien volumenes:

```bash
docker compose -f docker-compose.codespaces.yml down -v
```

## 6) Troubleshooting

### Error de recursos (lentitud o reinicios)

- Deja solo los servicios imprescindibles:

```bash
docker compose -f docker-compose.codespaces.yml stop python-env
```

- No ejecutes este perfil y el stack completo a la vez.

### El puerto abre pero no carga

- Revisa que el puerto este en `Public`.
- Revisa logs del servicio concreto.

### NameNode tarda mucho en arrancar

- Espera 1-2 minutos la primera vez (formateo inicial + inicializacion).

## 7) Recomendaciones para clase/demo

1. Arranca cluster 10 minutos antes.
2. Verifica UIs (9870/8080/8888).
3. Ten preparados comandos de logs para mostrar estado.
4. Si hay presion de recursos, para Jupyter y manten Hadoop+Spark+Kafka.
