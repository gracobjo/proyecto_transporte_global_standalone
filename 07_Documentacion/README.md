# Documentación — SIMLOG España

Índice de documentos técnicos del proyecto (modo **standalone**).

## Visión general

| Documento | Descripción |
|-----------|-------------|
| [ALINEACION_SENTINEL360_STANDALONE.md](ALINEACION_SENTINEL360_STANDALONE.md) | Equivalencias con [Sentinel360](https://github.com/gracobjo/Proyecto-Big-Data-Sentinel-360) y principios standalone |
| [DISENO_ARQUITECTURA.md](DISENO_ARQUITECTURA.md) | Arquitectura, componentes y decisiones |
| [DISENO_SISTEMA.md](DISENO_SISTEMA.md) | Diseño lógico, restricciones, diagramas Mermaid integrados |
| [CASOS_DE_USO.md](CASOS_DE_USO.md) | Actores, catálogo CU-01…CU-20, diagrama Mermaid |
| [CHANGELOG_DOCUMENTACION_2026_04.md](CHANGELOG_DOCUMENTACION_2026_04.md) | Cambios recientes en documentación (cuadro de mando, Hive `_P24H`, CQL) |
| [INTEGRACION_KNIME_HIVE.md](INTEGRACION_KNIME_HIVE.md) | KNIME ↔ Hive (JDBC), dataset ML, pipeline nodos, PMML, integración FastAPI; pestaña Streamlit **KNIME / IA avanzada** |
| [PRESENTACION_PROYECTO_SIMLOG.md](PRESENTACION_PROYECTO_SIMLOG.md) | Guion breve de presentación (demo, stack, novedades) |
| [MEMORIA_PROYECTO_SIMLOG.md](MEMORIA_PROYECTO_SIMLOG.md) | Documento único “en papel” para presentación formal (índice + consolidación documental) |
| [DASHBOARD_KDD_UI.md](DASHBOARD_KDD_UI.md) | **Diseño** de la pestaña Ciclo KDD (Streamlit): módulos, fases 1–2 / 3–5, seguridad API |
| [FLUJO_DATOS_Y_REQUISITOS.md](FLUJO_DATOS_Y_REQUISITOS.md) | Flujo de datos, JSON, Hive/Cassandra, **RF/RNF dashboard KDD** (§7–8) |
| [REQUIREMENTS_CHECKLIST.md](REQUIREMENTS_CHECKLIST.md) | Checklist frente al PDF del enunciado |
| [AIRFLOW.md](AIRFLOW.md) | Airflow sin Streamlit, DAGs, troubleshooting (colas, `base_url`) |
| [AIRFLOW_DAGS_SIMLOG.md](AIRFLOW_DAGS_SIMLOG.md) | Referencia operativa: qué hace cada DAG `simlog_*` (maestro, fases KDD y utilidades) |
| [YARN_Y_SPARK.md](YARN_Y_SPARK.md) | YARN vs Spark local |
| [CODESPACES_CLUSTER.md](CODESPACES_CLUSTER.md) | Perfil separado para montar clúster Big Data en Codespaces sin mezclar con el stack principal |
| [DIAGRAMAS_MERMAID.md](DIAGRAMAS_MERMAID.md) | **Diagramas UML en Mermaid** (casos de uso, componentes, secuencia, despliegue) |
| [MANUAL_USUARIO.md](MANUAL_USUARIO.md) | Manual completo de uso de pestañas, consultas, servicios y operación |
| [MANUAL_USUARIO_GRAFICO.md](MANUAL_USUARIO_GRAFICO.md) | Guía visual rápida: buscador semántico, servicios, informes PDF y flujo operativo |
| [MANUAL_DESARROLLADOR.md](MANUAL_DESARROLLADOR.md) | Extensión técnica, módulos, endpoints, seguridad SQL/CQL y UML |
| [NOTIFICACIONES_EMAIL_TELEGRAM.md](NOTIFICACIONES_EMAIL_TELEGRAM.md) | Correo SMTP (`SIMLOG_SMTP_*`) y Telegram (`SIMLOG_TELEGRAM_*`): variables, módulos y prueba manual |
| [ESTUDIO_CAMBIOS_UI_PIPELINE_2026_04.md](ESTUDIO_CAMBIOS_UI_PIPELINE_2026_04.md) | Estudio técnico de cambios recientes (UI, ingesta, Spark, NiFi, Hive/Airflow) con checklist de validación |

## Diagramas

| Formato | Ubicación |
|---------|-----------|
| **Mermaid (recomendado)** | [DIAGRAMAS_MERMAID.md](DIAGRAMAS_MERMAID.md), [DISENO_SISTEMA.md](DISENO_SISTEMA.md), [CASOS_DE_USO.md](CASOS_DE_USO.md) |
| PlantUML (legado) | `uml/casos_uso_simlog.puml`, `uml/componentes_simlog.puml`, `uml/secuencia_kdd_15min.puml` |

## Operación rápida del stack

```bash
cd ~/proyecto_transporte_global
source venv_transporte/bin/activate
python -u scripts/simlog_stack.py start   # | status | stop
```

## Referencia externa

- **Sentinel360**: [gracobjo/Proyecto-Big-Data-Sentinel-360](https://github.com/gracobjo/Proyecto-Big-Data-Sentinel-360) — ideas de funcionalidad; SIMLOG no exige clúster multi-nodo.
- **Persistencia operativa:** solo **Apache Cassandra** (no MongoDB).
