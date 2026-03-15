# Guía para varios agentes en el mismo código

Cuando más de una persona o agente trabaja en este repositorio, seguir estas reglas reduce conflictos y mantiene `main` estable.

---

## Asignación por componente

Cada agente debería priorizar **solo** los archivos/carpetas de su ámbito. Si necesitas tocar algo de otro ámbito, coordina o hazlo en una rama separada y abre PR.

| Ámbito | Archivos / carpetas | Responsabilidad |
|--------|---------------------|-----------------|
| **Ingesta** | `ingesta_kdd.py`, `ingesta/`, Kafka/HDFS en `config.py` | Clima, simulación, publicación a Kafka y HDFS. |
| **Procesamiento** | `procesamiento/`, `persistencia_hive.py`, JARs en `config.py` | Grafos, autosanación, Cassandra, Hive. |
| **Dashboard y docs** | `app_visualizacion.py`, `README.md`, `README_GEMELO_DIGITAL.md`, `AGENTS.md` | Mapa, métricas, documentación. |
| **Infra y orquestación** | `config_nodos.py`, `config.py`, `orquestacion/`, `cassandra/`, `setup_hive.hql` | Topología, configuración global, DAG, esquemas. |

---

## Reglas de ramas

1. **`main`** — Solo código integrado y estable. No hacer commit directo; todo entra por merge/PR.
2. **Rama por tarea** — Usar ramas cortas por feature o fix:
   - `feature/descripcion` — Nueva funcionalidad.
   - `fix/descripcion` — Corrección de bug.
   - `docs/descripcion` — Solo documentación.
3. **Antes de push** — Actualizar tu rama con `main`:
   ```bash
   git fetch origin
   git pull --rebase origin main
   ```
4. **Integrar por Pull Request** — Crear PR de tu rama a `main`; revisar y resolver conflictos en la rama antes de merge.

---

## Resumen

- Un agente por ámbito cuando sea posible.
- Una rama por tarea; integrar a `main` vía PR.
- Siempre `pull --rebase origin main` antes de seguir trabajando o abrir PR.
