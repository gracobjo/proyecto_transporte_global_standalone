"""
Compatibilidad histórica.

Este fichero en raíz existía como entrypoint de ingesta. Hoy la implementación canónica es:
  - `python -m ingesta.ingesta_kdd`  (módulo)
  - `ingesta/ingesta_kdd.py`        (script dentro del paquete)

Mantener un “shim” evita que Airflow/NiFi/documentación antigua se rompa y garantiza
que todos ejecutan la misma ingesta (Kafka + HDFS con fallback WebHDFS, DGT, etc.).
"""

from __future__ import annotations

from ingesta.ingesta_kdd import main
from ingesta.trigger_paso import resolver_paso_ingesta_detalle


if __name__ == "__main__":
    _p, _m = resolver_paso_ingesta_detalle()
    main(paso_15min=_p, paso_15min_modo=_m)
