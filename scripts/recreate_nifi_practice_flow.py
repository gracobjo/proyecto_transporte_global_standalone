#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import time
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

    def delete(self, path: str, *, params: dict[str, Any] | None = None) -> None:
        r = self.s.delete(f"{NIFI_API}{path}", headers=self.headers, params=params, timeout=30)
        r.raise_for_status()


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


def create_controller_service(
    client: NifiClient,
    pg_id: str,
    name: str,
    service_type: str,
    x: float,
    y: float,
) -> dict[str, Any]:
    services = client.get(f"/flow/process-groups/{pg_id}/controller-services").get("controllerServices", [])
    for svc in services:
        comp = svc["component"]
        if comp["name"] == name:
            return svc
    payload = {
        "revision": {"version": 0},
        "component": {
            "type": service_type,
            "name": name,
            "position": {"x": x, "y": y},
        },
    }
    return client.post(f"/process-groups/{pg_id}/controller-services", payload)


def get_controller_service(client: NifiClient, service_id: str) -> dict[str, Any]:
    return client.get(f"/controller-services/{service_id}")


def update_processor(client: NifiClient, entity: dict[str, Any], *, properties: dict[str, Any] | None = None,
                     scheduling_period: str | None = None, scheduling_strategy: str | None = None,
                     auto_terminated: list[str] | None = None,
                     position: tuple[float, float] | None = None) -> dict[str, Any]:
    comp = entity["component"]
    payload = {
        "revision": component_revision(entity),
        "component": {
            "id": comp["id"],
            "name": comp["name"],
            "position": comp.get("position", {"x": 0.0, "y": 0.0}),
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
    if position is not None:
        payload["component"]["position"] = {"x": float(position[0]), "y": float(position[1])}
    return client.put(f"/processors/{comp['id']}", payload)


def update_controller_service(
    client: NifiClient,
    entity: dict[str, Any],
    *,
    properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    comp = entity["component"]
    payload = {
        "revision": component_revision(entity),
        "component": {
            "id": comp["id"],
            "name": comp["name"],
            "properties": dict(comp.get("properties", {})),
        },
    }
    if properties:
        payload["component"]["properties"].update(properties)
    return client.put(f"/controller-services/{comp['id']}", payload)


def enable_controller_service(client: NifiClient, entity: dict[str, Any]) -> dict[str, Any]:
    comp = entity["component"]
    payload = {
        "revision": component_revision(entity),
        "state": "ENABLED",
    }
    return client.put(f"/controller-services/{comp['id']}/run-status", payload)


def disable_controller_service(client: NifiClient, entity: dict[str, Any]) -> dict[str, Any]:
    comp = entity["component"]
    payload = {
        "revision": component_revision(entity),
        "state": "DISABLED",
    }
    return client.put(f"/controller-services/{comp['id']}/run-status", payload)


def wait_for_controller_service_state(
    client: NifiClient,
    service_id: str,
    target_state: str,
    timeout_s: float = 15.0,
) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    latest = get_controller_service(client, service_id)
    while time.time() < deadline:
        latest = get_controller_service(client, service_id)
        if latest["component"].get("state") == target_state:
            return latest
        time.sleep(0.5)
    return latest


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


def drop_queue(client: NifiClient, connection_id: str) -> None:
    req = client.post(f"/flowfile-queues/{connection_id}/drop-requests", {})
    drop = req["dropRequest"]
    drop_id = drop["id"]
    while not drop.get("finished", False):
        drop = client.get(f"/flowfile-queues/{connection_id}/drop-requests/{drop_id}")["dropRequest"]
    client.delete(f"/flowfile-queues/{connection_id}/drop-requests/{drop_id}")


def remove_connection(client: NifiClient, pg_id: str, src_name: str, dst_name: str, relationship: str) -> None:
    flow = client.get(f"/flow/process-groups/{pg_id}")
    for conn in flow["processGroupFlow"]["flow"].get("connections", []):
        comp = conn["component"]
        if (
            comp["source"]["name"] == src_name
            and comp["destination"]["name"] == dst_name
            and relationship in comp.get("selectedRelationships", [])
        ):
            queued = conn.get("status", {}).get("aggregateSnapshot", {}).get("queuedCount")
            if queued not in (None, "0", 0):
                drop_queue(client, comp["id"])
            client.delete(f"/connections/{comp['id']}", params=component_revision(conn))


def remove_processor(client: NifiClient, pg_id: str, name: str) -> None:
    flow = client.get(f"/flow/process-groups/{pg_id}")
    for proc in flow["processGroupFlow"]["flow"].get("processors", []):
        comp = proc["component"]
        if comp["name"] != name:
            continue
        for conn in list(flow["processGroupFlow"]["flow"].get("connections", [])):
            c = conn["component"]
            if c["source"]["id"] == comp["id"] or c["destination"]["id"] == comp["id"]:
                queued = conn.get("status", {}).get("aggregateSnapshot", {}).get("queuedCount")
                if queued not in (None, "0", 0):
                    drop_queue(client, c["id"])
                client.delete(f"/connections/{c['id']}", params=component_revision(conn))
        client.delete(f"/processors/{comp['id']}", params=component_revision(proc))
        return


def main() -> None:
    client = NifiClient()
    root_id = client.get("/flow/process-groups/root")["processGroupFlow"]["id"]
    pg = create_pg(client, root_id, "PG_SIMLOG_KDD")
    pg_id = pg["component"]["id"]

    scripts_root = BASE / "nifi"
    owm_key = ENV.get("OWM_API_KEY") or ENV.get("API_WEATHER_KEY") or ""
    if not owm_key:
        raise SystemExit("No se encontro OWM_API_KEY/API_WEATHER_KEY en .env")

    p1 = create_processor(client, pg_id, "Timer_Ingesta_15min", "org.apache.nifi.processors.standard.GenerateFlowFile", 80, 180)
    p1 = update_processor(client, p1, properties={"Batch Size": "1"}, scheduling_period="15 min",
                          scheduling_strategy="TIMER_DRIVEN", auto_terminated=[],
                          position=(80, 180))

    p2 = create_processor(client, pg_id, "Set_Parametros_Ingesta", "org.apache.nifi.processors.attributes.UpdateAttribute", 380, 180)
    p2 = update_processor(
        client,
        p2,
        properties={
            "owm.api.key": owm_key,
            "owm.city.ids": "3117735,3128760,3128026,3105976,2510911",
            "owm.url": None,
            "dgt.url": "https://nap.dgt.es/datex2/v3/dgt/SituationPublication/datex2_v36.xml",
            "paso_15min": "0",
            "filename": "simlog_practice_${now():format(\"yyyyMMdd_HHmmss\")}.json",
        },
        auto_terminated=[],
        position=(380, 180),
    )

    p3 = create_processor(client, pg_id, "Build_GPS_Sintetico", "org.apache.nifi.processors.script.ExecuteScript", 700, 180)
    p3 = update_processor(
        client,
        p3,
        properties={
            "Script Engine": "Groovy",
            "Script File": str(scripts_root / "groovy" / "GenerateSyntheticGpsForPractice.groovy"),
        },
        auto_terminated=[],
        position=(700, 180),
    )

    p4 = create_processor(client, pg_id, "OpenWeather_InvokeHTTP", "org.apache.nifi.processors.standard.InvokeHTTP", 1040, 180)
    p4 = update_processor(
        client,
        p4,
        properties={
            "HTTP URL": "https://api.openweathermap.org/data/2.5/group?id=${owm.city.ids}&appid=${owm.api.key}&units=metric&lang=es",
            "Remote URL": None,
            "HTTP Method": "GET",
            "Response Body Attribute Name": "owm.response",
            "Always Output Response": None,
        },
        auto_terminated=["Response"],
        position=(1040, 180),
    )

    p5 = create_processor(client, pg_id, "Merge_Weather_Into_Payload", "org.apache.nifi.processors.script.ExecuteScript", 1380, 180)
    p5 = update_processor(
        client,
        p5,
        properties={
            "Script Engine": "Groovy",
            "Script File": str(scripts_root / "groovy" / "MergeOpenWeatherIntoPayload.groovy"),
        },
        auto_terminated=[],
        position=(1380, 180),
    )

    p6 = create_processor(client, pg_id, "DGT_DATEX2_InvokeHTTP", "org.apache.nifi.processors.standard.InvokeHTTP", 1720, 180)
    p6 = update_processor(
        client,
        p6,
        properties={
            "HTTP URL": "${dgt.url}",
            "Remote URL": None,
            "HTTP Method": "GET",
            "Response Body Attribute Name": "dgt.response.xml",
            "Always Output Response": None,
        },
        auto_terminated=["Response"],
        position=(1720, 180),
    )

    p7 = create_processor(client, pg_id, "Merge_DGT_Into_Payload", "org.apache.nifi.processors.script.ExecuteScript", 2060, 180)
    p7 = update_processor(
        client,
        p7,
        properties={
            "Script Engine": "Groovy",
            "Script File": str(scripts_root / "groovy" / "MergeDgtDatex2IntoPayload.groovy"),
        },
        auto_terminated=[],
        position=(2060, 180),
    )

    kafka_service = create_controller_service(
        client,
        pg_id,
        "Kafka_SIMLOG_Local",
        "org.apache.nifi.kafka.service.Kafka3ConnectionService",
        1700,
        -260,
    )
    kafka_service = wait_for_controller_service_state(client, kafka_service["component"]["id"], "DISABLED")
    if kafka_service["component"].get("state") != "DISABLED":
        kafka_service = disable_controller_service(client, kafka_service)
        kafka_service = wait_for_controller_service_state(client, kafka_service["component"]["id"], "DISABLED")
    kafka_service = update_controller_service(
        client,
        kafka_service,
        properties={
            "bootstrap.servers": "localhost:9092",
        },
    )
    kafka_service = enable_controller_service(client, kafka_service)

    p8 = create_processor(client, pg_id, "Kafka_Publish_DGT_RAW", "org.apache.nifi.kafka.processors.PublishKafka", 2440, 20)
    p8 = update_processor(
        client,
        p8,
        properties={
            "Bootstrap Servers": None,
            "Kafka Connection Service": kafka_service["component"]["id"],
            "Topic Name": "transporte_dgt_raw",
            "Transactions Enabled": "false",
        },
        auto_terminated=["success"],
        position=(2440, 20),
    )

    p9 = create_processor(client, pg_id, "Kafka_Publish_RAW", "org.apache.nifi.kafka.processors.PublishKafka", 2440, 160)
    p9 = update_processor(
        client,
        p9,
        properties={
            "Bootstrap Servers": None,
            "Kafka Connection Service": kafka_service["component"]["id"],
            "Topic Name": "transporte_raw",
            "Transactions Enabled": "false",
        },
        auto_terminated=["success"],
        position=(2440, 160),
    )

    p10 = create_processor(client, pg_id, "Kafka_Publish_FILTERED", "org.apache.nifi.kafka.processors.PublishKafka", 2440, 300)
    p10 = update_processor(
        client,
        p10,
        properties={
            "Bootstrap Servers": None,
            "Kafka Connection Service": kafka_service["component"]["id"],
            "Topic Name": "transporte_filtered",
            "Transactions Enabled": "false",
        },
        auto_terminated=[],
        position=(2440, 300),
    )

    p11 = create_processor(client, pg_id, "HDFS_Backup_JSON", "org.apache.nifi.processors.standard.ExecuteStreamCommand", 2440, 440)
    p11 = update_processor(
        client,
        p11,
        properties={
            "Command Path": "/bin/bash",
            "Command Arguments": str(scripts_root / "scripts" / "put_hdfs_from_stdin.sh"),
            "Ignore STDIN": "false",
            "Output Destination Attribute": "hdfs.command.output",
        },
        auto_terminated=["original"],
        position=(2440, 440),
    )

    p13 = create_processor(client, pg_id, "Spark_Submit_Procesamiento", "org.apache.nifi.processors.standard.ExecuteProcess", 2800, 300)
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
        position=(2800, 300),
    )

    p14 = create_processor(client, pg_id, "Log_Fallos", "org.apache.nifi.processors.standard.LogAttribute", 2060, 600)
    p14 = update_processor(
        client,
        p14,
        properties={"Log Level": "error"},
        auto_terminated=["success"],
        position=(2060, 600),
    )

    remove_connection(client, pg_id, "Merge_Weather_Into_Payload", "Kafka_Publish_RAW", "success")
    remove_connection(client, pg_id, "Merge_Weather_Into_Payload", "Kafka_Publish_FILTERED", "success")
    remove_connection(client, pg_id, "Merge_Weather_Into_Payload", "HDFS_Backup_JSON", "success")
    remove_connection(client, pg_id, "Kafka_Publish_FILTERED", "Kafka_Consume_Filtered_for_Spark", "success")
    remove_connection(client, pg_id, "Kafka_Consume_Filtered_for_Spark", "Spark_Submit_Procesamiento", "success")
    remove_connection(client, pg_id, "Kafka_Publish_FILTERED", "Spark_Submit_Procesamiento", "success")
    remove_connection(client, pg_id, "HDFS_Backup_JSON", "Log_Fallos", "failure")
    remove_connection(client, pg_id, "Spark_Submit_Procesamiento", "Log_Fallos", "failure")
    remove_processor(client, pg_id, "Kafka_Consume_Filtered_for_Spark")
    p10 = update_processor(client, p10, auto_terminated=["success"], position=(2440, 300))

    connect(client, pg_id, p1, p2, "success")
    connect(client, pg_id, p2, p3, "success")
    connect(client, pg_id, p3, p4, "success")
    connect(client, pg_id, p4, p5, "Original")
    connect(client, pg_id, p4, p5, "Failure")
    connect(client, pg_id, p4, p5, "No Retry")
    connect(client, pg_id, p5, p6, "success")
    connect(client, pg_id, p6, p7, "Original")
    connect(client, pg_id, p7, p8, "success")
    connect(client, pg_id, p7, p9, "success")
    connect(client, pg_id, p7, p10, "success")
    connect(client, pg_id, p7, p11, "success")

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
        (p8, "failure"),
        (p9, "failure"),
        (p10, "failure"),
    ]:
        try:
            connect(client, pg_id, src, p14, rel)
        except Exception:
            pass

    print(json.dumps({"status": "ok", "process_group": "PG_SIMLOG_KDD", "process_group_id": pg_id}, indent=2))


if __name__ == "__main__":
    main()
