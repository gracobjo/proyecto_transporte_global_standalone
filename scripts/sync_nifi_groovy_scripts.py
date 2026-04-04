#!/usr/bin/env python3
"""
Solo actualiza los tres ExecuteScript (Groovy) de PG_SIMLOG_KDD: Script File + Script Body vacío.

Útil tras editar ``nifi/groovy/*.groovy`` sin ejecutar el recreate completo del flujo.

  python scripts/sync_nifi_groovy_scripts.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from scripts.recreate_nifi_practice_flow import (  # noqa: E402
    NIFI_PG_EXECUTE_SCRIPTS,
    NifiClient,
    assert_nifi_groovy_sources_exist,
    create_pg,
    get_processor_entity_by_name,
    halt_process_group_processors,
    restore_processors_disabled_to_stopped,
    update_processor,
    wait_for_nifi_api_ready,
)


def main() -> None:
    wait_for_nifi_api_ready()
    client = NifiClient()
    root_id = client.get("/flow/process-groups/root")["processGroupFlow"]["id"]
    pg = create_pg(client, root_id, "PG_SIMLOG_KDD")
    pg_id = pg["component"]["id"]
    scripts_root = ROOT / "nifi"
    assert_nifi_groovy_sources_exist(scripts_root)

    halt_process_group_processors(client, pg_id)
    updated: list[str] = []
    for proc_name, filename, x, y in NIFI_PG_EXECUTE_SCRIPTS:
        ent = get_processor_entity_by_name(client, pg_id, proc_name)
        if ent is None:
            print(
                json.dumps(
                    {
                        "status": "error",
                        "message": (
                            f"No existe el procesador «{proc_name}». "
                            "Ejecuta primero: python scripts/recreate_nifi_practice_flow.py"
                        ),
                        "process_group_id": pg_id,
                    },
                    indent=2,
                ),
                file=sys.stderr,
            )
            sys.exit(1)
        groovy_path = (scripts_root / "groovy" / filename).resolve()
        update_processor(
            client,
            ent,
            properties={
                "Script Engine": "Groovy",
                "Script File": str(groovy_path),
                "Script Body": "",
            },
            concurrent_tasks="2",
            auto_terminated=[],
            position=(x, y),
        )
        updated.append(proc_name)
    restore_processors_disabled_to_stopped(client, pg_id)
    print(json.dumps({"status": "ok", "updated_execute_scripts": updated}, indent=2))


if __name__ == "__main__":
    main()
