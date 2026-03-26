# Cluster Big Data en Codespaces (perfil separado)

Esta guia usa archivos **separados** del stack principal para evitar confusiones:

- `docker-compose.codespaces.yml`
- `Dockerfile.codespaces`
- `hadoop.codespaces.env`

No sustituye `docker-compose.yml` ni `Dockerfile` del proyecto principal.

## 1) Arranque del cluster en Codespaces

Desde la raiz del repositorio:

```bash
docker compose -f docker-compose.codespaces.yml up -d --build
docker compose -f docker-compose.codespaces.yml ps
```

## 2) Puertos que debes poner en Public

En la pestana **Ports** de Codespaces, marca como Public:

- `9870` (Hadoop NameNode UI)
- `8080` (Spark Master UI)
- `8888` (Jupyter / Notebook)

Opcional:

- `9092` (Kafka, normalmente no necesitas abrirlo al navegador)
- `9000` (HDFS RPC, no web)
- `7077` (Spark endpoint, no web)

## 3) Validaciones rapidas

```bash
docker compose -f docker-compose.codespaces.yml logs --tail=100 namenode
docker compose -f docker-compose.codespaces.yml logs --tail=100 spark-master
docker compose -f docker-compose.codespaces.yml logs --tail=100 kafka
```

Comprobaciones funcionales:

- Hadoop UI en `9870` accesible.
- Spark UI en `8080` accesible.
- Jupyter en `8888` con token en logs de `python-env`.

## 4) Parada y limpieza

Parar:

```bash
docker compose -f docker-compose.codespaces.yml down
```

Parar y borrar volumenes del perfil Codespaces:

```bash
docker compose -f docker-compose.codespaces.yml down -v
```

## 5) Notas de recursos (cuenta free)

- Empieza con este perfil ligero (1 DataNode, 1 Spark Worker).
- Si hay lentitud, para Jupyter temporalmente:

```bash
docker compose -f docker-compose.codespaces.yml stop python-env
```

- Evita levantar a la vez este perfil y el stack completo del proyecto.
