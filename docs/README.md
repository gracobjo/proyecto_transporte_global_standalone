# Documentación — SIMLOG España

Índice de documentos técnicos del proyecto (modo **standalone**).

## Visión general

| Documento | Descripción |
|-----------|-------------|
| [ALINEACION_SENTINEL360_STANDALONE.md](ALINEACION_SENTINEL360_STANDALONE.md) | Equivalencias con [Sentinel360](https://github.com/gracobjo/Proyecto-Big-Data-Sentinel-360) y principios standalone |
| [DISENO_ARQUITECTURA.md](DISENO_ARQUITECTURA.md) | Arquitectura, componentes y decisiones |
| [DISENO_SISTEMA.md](DISENO_SISTEMA.md) | Diseño lógico, restricciones, diagramas Mermaid integrados |
| [CASOS_DE_USO.md](CASOS_DE_USO.md) | Actores, catálogo CU-01…CU-09, diagrama Mermaid |
| [DASHBOARD_KDD_UI.md](DASHBOARD_KDD_UI.md) | **Diseño** de la pestaña Ciclo KDD (Streamlit): módulos, fases 1–2 / 3–5, seguridad API |
| [FLUJO_DATOS_Y_REQUISITOS.md](FLUJO_DATOS_Y_REQUISITOS.md) | Flujo de datos, JSON, Hive/Cassandra, **RF/RNF dashboard KDD** (§7–8) |
| [REQUIREMENTS_CHECKLIST.md](REQUIREMENTS_CHECKLIST.md) | Checklist frente al PDF del enunciado |
| [AIRFLOW.md](AIRFLOW.md) | Airflow sin Streamlit, DAGs, troubleshooting (colas, `base_url`) |
| [YARN_Y_SPARK.md](YARN_Y_SPARK.md) | YARN vs Spark local |
| [DIAGRAMAS_MERMAID.md](DIAGRAMAS_MERMAID.md) | **Diagramas UML en Mermaid** (casos de uso, componentes, secuencia, despliegue) |

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
