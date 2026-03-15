#!/bin/bash
# Crear temas Kafka: raw (crudos) y filtered (filtrados) según PDF Proyecto Big Data
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
for topic in transporte_raw transporte_filtered; do
  kafka-topics.sh --create --topic "$topic" --bootstrap-server "$BOOTSTRAP" --partitions 2 --replication-factor 1 2>/dev/null || \
  kafka-topics.sh --alter --topic "$topic" --bootstrap-server "$BOOTSTRAP" 2>/dev/null || true
done
echo "Temas: transporte_raw (crudos), transporte_filtered (filtrados)"
kafka-topics.sh --list --bootstrap-server "$BOOTSTRAP"
