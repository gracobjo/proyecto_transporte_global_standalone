FROM python:3.11-slim

# Variables de entorno básicas
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0

WORKDIR /app

# Java + cliente HDFS (para ingesta que ejecuta hdfs dfs)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    default-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Cliente Hadoop (solo binarios para hdfs dfs)
ENV HADOOP_VER=3.3.6
RUN curl -sL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VER}/hadoop-${HADOOP_VER}.tar.gz" | tar xz -C /opt \
    && mv "/opt/hadoop-${HADOOP_VER}" /opt/hadoop \
    && rm -rf /opt/hadoop/share/doc /opt/hadoop/share/hadoop/common/jars/*javadoc*
ENV HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/conf \
    PATH="/opt/hadoop/bin:$PATH"
RUN mkdir -p "$HADOOP_CONF_DIR"

# Copiar dependencias Python
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copiar el resto del proyecto
COPY . .

# Entrypoint: genera core-site.xml desde HDFS_NAMENODE y ejecuta el comando
# Convertir CRLF -> LF por si se edita en Windows
COPY entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh && chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

# Puerto del dashboard de Streamlit
EXPOSE 8501

# Variables por defecto (sobrescribir con -e o docker-compose)
ENV KAFKA_BOOTSTRAP=localhost:9092 \
    CASSANDRA_HOST=127.0.0.1 \
    HDFS_BACKUP_PATH=/user/hadoop/transporte_backup

# Comando por defecto: lanzar el dashboard
CMD ["streamlit", "run", "app_visualizacion.py", "--server.port=8501", "--server.address=0.0.0.0"]

