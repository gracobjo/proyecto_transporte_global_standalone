FROM apache/airflow:3.1.8

# Instalamos utilidades mínimas para pruebas de red
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Volvemos al usuario airflow recomendado por la imagen oficial
USER airflow

# AIRFLOW_HOME dentro del contenedor
ENV AIRFLOW_HOME=/opt/airflow

# Copiamos las dependencias del proyecto (opcional, muchas ya estarán en la imagen)
COPY requirements.txt /requirements-project.txt
RUN pip install --no-cache-dir -r /requirements-project.txt || true

# El comando final se define desde docker-compose (scheduler + api-server)
