#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE = Path(__file__).resolve().parent.parent
NIFI_API = os.environ.get("SIMLOG_NIFI_API", "https://localhost:8443/nifi-api").rstrip("/")
NIFI_USER = os.environ.get("SIMLOG_NIFI_USER", "nifi")
NIFI_PASSWORD = os.environ.get("SIMLOG_NIFI_PASSWORD", "nifinifinifi")


def cargar_env(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


ENV = cargar_env(BASE / ".env")


class NifiClient:
    def __init__(self) -> None:
        self.s = requests.Session()
        self.s.verify = False
        r = self.s.post(
            f"{NIFI_API}/access/token",
            data={"username": NIFI_USER, "password": NIFI_PASSWORD},
            timeout=20,
        )
        r.raise_for_status()
        self.headers = {"Authorization": f"Bearer {r.text}"}

    def get(self, path: str) -> dict[str, Any]:
        r = self.s.get(f"{NIFI_API}{path}", headers=self.headers, timeout=30)
        r.raise_for_status()
        return r.json()

    def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        r = self.s.post(f"{NIFI_API}{path}", headers=self.headers, json=payload, timeout=30)
        r.raise_for_status()
        return r.json()

    def put(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        r = self.s.put(f"{NIFI_API}{path}", headers=self.headers, json=payload, timeout=30)
        r.raise_for_status()
        return r.json()


def component_revision(entity: dict[str, Any]) -> dict[str, Any]:
    rev = entity["revision"]
    return {"clientId": rev.get("clientId"), "version": rev["version"]}


def create_pg(client: NifiClient, root_id: str, name: str) -> dict[str, Any]:
    existing = client.get("/process-groups/root/process-groups").get("processGroups", [])
    for pg in existing:
        comp = pg["component"]
        if comp["name"] == name:
            return pg
    payload = {
        "revision": {"version": 0},
        "component": {
            "name": name,
            "position": {"x": 200.0, "y": 200.0},
        },
    }
    return client.post(f"/process-groups/{root_id}/process-groups", payload)


def create_processor(client: NifiClient, pg_id: str, name: str, proc_type: str, x: float, y: float) -> dict[str, Any]:
    flow = client.get(f"/flow/process-groups/{pg_id}")
    for p in flow["processGroupFlow"]["flow"].get("processors", []):
        if p["component"]["name"] == name:
            return p
    payload = {
        "revision": {"version": 0},
        "component": {
            "type": proc_type,
            "name": name,
            "position": {"x": x, "y": y},
        },
    }
    return client.post(f"/process-groups/{pg_id}/processors", payload)


def update_processor(client: NifiClient, entity: dict[str, Any], *, properties: dict[str, str] | None = None,
                     scheduling_period: str | None = None, scheduling_strategy: str | None = None,
                     auto_terminated: list[str] | None = None) -> dict[str, Any]:
    comp = entity["component"]
    payload = {
        "revision": component_revision(entity),
        "component": {
            "id": comp["id"],
            "name": comp["name"],
            "position": comp["position"],
            "config": dict(comp.get("config", {})),
        },
    }
    cfg = payload["component"]["config"]
    cfg["properties"] = dict(comp.get("config", {}).get("properties", {}))
    if properties:
        cfg["properties"].update(properties)
    if scheduling_period:
        cfg["schedulingPeriod"] = scheduling_period
    if scheduling_strategy:
        cfg["schedulingStrategy"] = scheduling_strategy
    if auto_terminated is not None:
        cfg["autoTerminatedRelationships"] = auto_terminated
    return client.put(f"/processors/{comp['id']}", payload)


def connect(client: NifiClient, pg_id: str, source: dict[str, Any], dest: dict[str, Any], relationship: str) -> None:
    flow = client.get(f"/flow/process-groups/{pg_id}")
    src_id = source["component"]["id"]
    dst_id = dest["component"]["id"]
    for c in flow["processGroupFlow"]["flow"].get("connections", []):
        comp = c["component"]
        if comp["source"]["id"] == src_id and comp["destination"]["id"] == dst_id and relationship in comp.get("selectedRelationships", []):
            return
    payload = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "groupId": pg_id, "type": "PROCESSOR"},
            "destination": {"id": dst_id, "groupId": pg_id, "type": "PROCESSOR"},
            "selectedRelationships": [relationship],
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
        },
    }
    client.post(f"/process-groups/{pg_id}/connections", payload)


def main() -> None:
    client = NifiClient()
    root_id = client.get("/flow/process-groups/root")["processGroupFlow"]["id"]
    pg = create_pg(client, root_id, "PG_SIMLOG_KDD")
    pg_id = pg["component"]["id"]

    scripts_root = BASE / "nifi"
    owm_key = ENV.get("OWM_API_KEY") or ENV.get("API_WEATHER_KEY") or ""
    if not owm_key:
        raise SystemExit("No se encontro OWM_API_KEY/API_WEATHER_KEY en .env")

    p1 = create_processor(client, pg_id, "Timer_Ingesta_15min", "org.apache.nifi.processors.standard.GenerateFlowFile", 40, 80)
    p1 = update_processor(client, p1, properties={"Batch Size": "1"}, scheduling_period="15 min",
                          scheduling_strategy="TIMER_DRIVEN", auto_terminated=[])

    p2 = create_processor(client, pg_id, "Set_Parametros_Ingesta", "org.apache.nifi.processors.attributes.UpdateAttribute", 280, 80)
    p2 = update_processor(
        client,
        p2,
        properties={
            "owm.api.key": owm_key,
            "owm.city.ids": "3117735,3128760,3128026,3105976,2510911",
            "owm.url": "https://api.openweathermap.org/data/2.5/group?id=${owm.city.ids}&appid=${owm.api.key}&units=metric&lang=es",
            "dgt.url": "https://nap.dgt.es/datex2/v3/dgt/SituationPublication/datex2_v36.xml",
            "paso_15min": "0",
            "filename": "simlog_practice_${now():format(\"yyyyMMdd_HHmmss\")}.json",
        },
        auto_terminated=[],
    )

    p3 = create_processor(client, pg_id, "Build_GPS_Sintetico", "org.apache.nifi.processors.script.ExecuteScript", 540, 80)
    p3 = update_processor(
        client,
        p3,
        properties={
            "Script Engine": "Groovy",
            "Script File": str(scripts_root / "groovy" / "GenerateSyntheticGpsForPractice.groovy"),
        },
        auto_terminated=[],
    )

    p4 = create_processor(client, pg_id, "OpenWeather_InvokeHTTP", "org.apache.nifi.processors.standard.InvokeHTTP", 800, 80)
    p4 = update_processor(
        client,
        p4,
        properties={
            "Remote URL": "${owm.url}",
            "HTTP Method": "GET",
            "Response Body Attribute Name": "owm.response",
            "Always Output Response": "false",
        },
        auto_terminated=["response"],
    )

    p5 = create_processor(client, pg_id, "Merge_Weather_Into_Payload", "org.apache.nifi.processors.script.ExecuteScript", 1080, 80)
    p5 = update_processor(
        client,
        p5,
        properties={
            "Script Engine": "Groovy",
            "Script File": str(scripts_root / "groovy" / "MergeOpenWeatherIntoPayload.groovy"),
        },
        auto_terminated=[],
    )

    p6 = create_processor(client, pg_id, "DGT_DATEX2_InvokeHTTP", "org.apache.nifi.processors.standard.InvokeHTTP", 1320, 80)
    p6 = update_processor(
        client,
        p6,
        properties={
            "Remote URL": "${dgt.url}",
            "HTTP Method": "GET",
            "Response Body Attribute Name": "dgt.response.xml",
            "Always Output Response": "false",
        },
        auto_terminated=["response"],
    )

    p7 = create_processor(client, pg_id, "Merge_DGT_Into_Payload", "org.apache.nifi.processors.script.ExecuteScript", 1560, 80)
    p7 = update_processor(
        client,
        p7,
        properties={
            "Script Engine": "Groovy",
            "Script File": str(scripts_root / "groovy" / "MergeDgtDatex2IntoPayload.groovy"),
        },
        auto_terminated=[],
    )

    p8 = create_processor(client, pg_id, "Kafka_Publish_DGT_RAW", "org.apache.nifi.kafka.processors.PublishKafka", 1820, -80)
    p8 = update_processor(
        client,
        p8,
        properties={
            "Bootstrap Servers": "localhost:9092",
            "Topic Name": "transporte_dgt_raw",
        },
        auto_terminated=["success"],
    )

    p9 = create_processor(client, pg_id, "Kafka_Publish_RAW", "org.apache.nifi.kafka.processors.PublishKafka", 1820, 20)
    p9 = update_processor(
        client,
        p9,
        properties={
            "Bootstrap Servers": "localhost:9092",
            "Topic Name": "transporte_raw",
        },
        auto_terminated=["success"],
    )

    p10 = create_processor(client, pg_id, "Kafka_Publish_FILTERED", "org.apache.nifi.kafka.processors.PublishKafka", 1820, 120)
    p10 = update_processor(
        client,
        p10,
        properties={
            "Bootstrap Servers": "localhost:9092",
            "Topic Name": "transporte_filtered",
        },
        auto_terminated=[],
    )

    p11 = create_processor(client, pg_id, "HDFS_Backup_JSON", "org.apache.nifi.processors.standard.ExecuteStreamCommand", 1820, 250)
    p11 = update_processor(
        client,
        p11,
        properties={
            "Command Path": "/bin/bash",
            "Command Arguments": str(scripts_root / "scripts" / "put_hdfs_from_stdin.sh"),
            "Ignore STDIN": "false",
            "Output Destination Attribute": "hdfs.command.output",
        },
        auto_terminated=["output stream", "nonzero status"],
    )

    p13 = create_processor(client, pg_id, "Spark_Submit_Procesamiento", "org.apache.nifi.processors.standard.ExecuteProcess", 2080, 120)
    p13 = update_processor(
        client,
        p13,
        properties={
            "Command": "/bin/bash",
            "Command Arguments": "-lc \"PROJECT_ROOT='{}' SPARK_MASTER='local' CASSANDRA_HOST='127.0.0.1' {}\"".format(
                str(BASE),
                str(scripts_root / "scripts" / "spark_submit_yarn.sh"),
            ),
        },
        auto_terminated=["success"],
    )

    p14 = create_processor(client, pg_id, "Log_Fallos", "org.apache.nifi.processors.standard.LogAttribute", 2080, 320)
    p14 = update_processor(client, p14, properties={"Log Level": "error"}, auto_terminated=["success"])

    connect(client, pg_id, p1, p2, "success")
    connect(client, pg_id, p2, p3, "success")
    connect(client, pg_id, p3, p4, "success")
    connect(client, pg_id, p4, p5, "Original")
    connect(client, pg_id, p5, p6, "success")
    connect(client, pg_id, p6, p7, "Original")
    connect(client, pg_id, p7, p8, "success")
    connect(client, pg_id, p7, p9, "success")
    connect(client, pg_id, p7, p10, "success")
    connect(client, pg_id, p7, p11, "success")
    connect(client, pg_id, p10, p13, "success")

    for src, rel in [
        (p3, "failure"),
        (p4, "Failure"),
        (p4, "Retry"),
        (p4, "No Retry"),
        (p5, "failure"),
        (p6, "Failure"),
        (p6, "Retry"),
        (p6, "No Retry"),
        (p7, "failure"),
        (p11, "failure"),
        (p13, "failure"),
    ]:
        try:
            connect(client, pg_id, src, p14, rel)
        except Exception:
            pass

    print(json.dumps({"status": "ok", "process_group": "PG_SIMLOG_KDD", "process_group_id": pg_id}, indent=2))


if __name__ == "__main__":
    main()
