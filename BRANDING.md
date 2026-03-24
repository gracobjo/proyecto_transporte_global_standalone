# Marca del proyecto: SIMLOG España

## Nombre propuesto

**SIMLOG España** — *Sistema Integrado de Monitorización y Simulación Logística*

### Por qué sustituye a «gemelo digital»

- **Monitorización:** seguimiento del estado de la red (nodos, aristas, camiones, clima).
- **Simulación:** incidentes y GPS sintéticos en el ciclo KDD.
- **España:** topología y hubs reales en el territorio.

El término genérico «gemelo digital» no describe el dominio (logística, transporte, riesgos); **SIMLOG** es reconocible en documentación, Airflow, NiFi y la UI.

## Uso en código

Constantes en `config.py`:

- `PROJECT_DISPLAY_NAME` — títulos Streamlit (personalizable con env `PROJECT_DISPLAY_NAME`)
- `PROJECT_SLUG` — `simlog_es` (tags, identificadores)
- `PROJECT_TAGLINE` — subtítulo del dashboard
- `PROJECT_DESCRIPTION` — descripción larga
- `NIFI_PROCESS_GROUP_NAME` — grupo NiFi recomendado: `PG_SIMLOG_KDD`

## Renombrados recientes

| Antes | Ahora |
|-------|--------|
| `README_GEMELO_DIGITAL.md` | `README_SIMLOG.md` |
| `nifi/flow/gemelo_digital_flow_spec.yaml` | `nifi/flow/simlog_kdd_flow_spec.yaml` |
| DAG Airflow `dag_maestro_transporte` | `simlog_pipeline_maestro` |

Si en Airflow ya tenías el DAG antiguo, desactívalo y usa el nuevo id tras actualizar `orquestacion/dag_maestro.py`.
