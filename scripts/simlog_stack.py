#!/usr/bin/env python3
"""
CLI para arranque secuencial, comprobación y parada secuencial del stack SIMLOG.
Usado por scripts/arrancar_stack.sh, comprobar_stack.sh y parar_stack.sh.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from servicios.gestion_servicios import (  # noqa: E402
    arrancar_todos_servicios,
    comprobar_todos,
    ejecutar_iniciar,
    esperar_cassandra,
    esperar_hiveserver2,
    ensure_hiveserver2,
    parar_todos_servicios,
)


def main() -> int:
    p = argparse.ArgumentParser(description="Gestión secuencial del stack SIMLOG")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("start", help="Arranca servicios uno tras otro (orden definido en gestion_servicios)")
    sub.add_parser("status", help="Lista estado de cada servicio")
    sub.add_parser("stop", help="Para servicios uno tras otro (orden inverso)")
    sub.add_parser(
        "ensure-hive",
        help="Asegura HiveServer2 (JDBC) y metastore; útil si 9083 está arriba y 10000 no",
    )

    args = p.parse_args()

    if args.cmd == "start":
        print(
            "Arranque secuencial del stack (salida por paso; pasos largos = normal).",
            flush=True,
        )
        for line in arrancar_todos_servicios(verbose=True):
            print(line, flush=True)

        print(
            "→ Comprobación de Cassandra (puerto CQL 9042; ya debería estar tras el paso cassandra)…",
            flush=True,
        )
        ok_cassandra, msg_cassandra = esperar_cassandra(60)
        print(f"cassandra: {msg_cassandra}", flush=True)
        if not ok_cassandra:
            print(
                "→ Reintento de arranque de Cassandra y nueva espera…",
                flush=True,
            )
            print(ejecutar_iniciar("cassandra"), flush=True)
            ok_cassandra, msg_cassandra = esperar_cassandra(120)
            print(f"cassandra: {msg_cassandra}", flush=True)
        if not ok_cassandra:
            return 1

        # Hive a menudo supera el primer ciclo de espera si el metastore tarda; reforzamos aquí.
        print(
            "→ Espera y comprobación de HiveServer2 (puerto JDBC; puede tardar varios minutos)…",
            flush=True,
        )
        ok_hive, msg_hive = esperar_hiveserver2(240)
        print(f"hive: {msg_hive}", flush=True)
        if not ok_hive:
            print(
                "→ Reintento de arranque de HiveServer2 y nueva espera…",
                flush=True,
            )
            print(ejecutar_iniciar("hive"), flush=True)
            ok_hive, msg_hive = esperar_hiveserver2(180)
            print(f"hive: {msg_hive}", flush=True)

        return 0 if ok_hive else 1

    if args.cmd == "status":
        for r in comprobar_todos():
            ok = "OK" if r.get("activo") else "no"
            print(f"{r['id']}: {ok} — {r.get('detalle', '')}")
        return 0

    if args.cmd == "ensure-hive":
        print(ensure_hiveserver2(), flush=True)
        ok_h, msg_h = esperar_hiveserver2(60)
        print(f"comprobación: {msg_h}", flush=True)
        return 0 if ok_h else 1

    if args.cmd == "stop":
        print("Parada secuencial del stack…", flush=True)
        for line in parar_todos_servicios(verbose=True):
            print(line, flush=True)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
