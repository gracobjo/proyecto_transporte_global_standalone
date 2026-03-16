#!/bin/sh
# Genera core-site.xml para el cliente HDFS segun HDFS_NAMENODE y ejecuta el comando
set -e
if [ -n "$HDFS_NAMENODE" ] && [ -d "$HADOOP_CONF_DIR" ]; then
  cat > "$HADOOP_CONF_DIR/core-site.xml" << EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://${HDFS_NAMENODE}</value>
  </property>
  <property>
    <name>dfs.client.use.datanode.hostname</name>
    <value>false</value>
  </property>
</configuration>
EOF
fi
exec "$@"
