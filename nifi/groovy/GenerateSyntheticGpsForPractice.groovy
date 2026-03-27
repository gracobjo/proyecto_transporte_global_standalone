/**
 * NiFi ExecuteScript (Groovy) - GPS sintetico para la practica.
 *
 * Genera un payload inicial con:
 * - timestamp
 * - paso_15min
 * - estados_nodos sinteticos
 * - estados_aristas sinteticos
 * - camiones con GPS sintetico
 * - clima_hubs vacio (se rellena despues con InvokeHTTP)
 */

import groovy.json.JsonOutput
import org.apache.nifi.processor.io.OutputStreamCallback

def flowFile = session.get()
if (!flowFile) return

def rnd = java.util.concurrent.ThreadLocalRandom.current()
def pasoStr = flowFile.getAttribute("paso_15min")
def paso = (pasoStr != null && !pasoStr.isEmpty()) ? pasoStr.toInteger() : 0

def hubs = [
    [id: "Madrid", lat: 40.4168d, lon: -3.7038d],
    [id: "Barcelona", lat: 41.3851d, lon: 2.1734d],
    [id: "Bilbao", lat: 43.2630d, lon: -2.9350d],
    [id: "Vigo", lat: 42.2406d, lon: -8.7207d],
    [id: "Sevilla", lat: 37.3891d, lon: -5.9845d]
]

def haversineKm = { lat1, lon1, lat2, lon2 ->
    double r = 6371.0d
    double dLat = Math.toRadians(lat2 - lat1)
    double dLon = Math.toRadians(lon2 - lon1)
    double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
        Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
        Math.sin(dLon / 2) * Math.sin(dLon / 2)
    return r * 2.0d * Math.atan2(Math.sqrt(a), Math.sqrt(1.0d - a))
}

def estados = ["OK", "Congestionado", "Bloqueado"]
def severityFor = { String estado ->
    if (estado == "Bloqueado") return "high"
    if (estado == "Congestionado") return "medium"
    return "low"
}
def estadosNodos = [:]
hubs.each { h ->
    def estado = estados[rnd.nextInt(estados.size())]
    estadosNodos[h.id] = [
        estado: estado,
        motivo: estado == "OK" ? "" : "Simulado NiFi",
        source: "simulacion",
        severity: severityFor(estado),
        peso_pagerank: 1.0d
    ]
}

def aristasEstado = [:]
for (int i = 0; i < hubs.size(); i++) {
    for (int j = i + 1; j < hubs.size(); j++) {
        def src = hubs[i]
        def dst = hubs[j]
        def estado = estados[rnd.nextInt(estados.size())]
        aristasEstado["${src.id}|${dst.id}"] = [
            estado: estado,
            motivo: estado == "OK" ? "" : "Simulado NiFi",
            distancia_km: 100 + rnd.nextInt(600),
            source: "simulacion",
            severity: severityFor(estado)
        ]
    }
}

def camiones = (1..5).collect { i ->
    def origen = hubs[rnd.nextInt(hubs.size())]
    def destino = hubs[rnd.nextInt(hubs.size())]
    while (destino.id == origen.id) {
        destino = hubs[rnd.nextInt(hubs.size())]
    }
    def lat = origen.lat + ((rnd.nextDouble() - 0.5d) * 0.18d)
    def lon = origen.lon + ((rnd.nextDouble() - 0.5d) * 0.18d)
    def ruta = [origen.id, destino.id]
    def distanciaTotal = Math.round(haversineKm(origen.lat, origen.lon, destino.lat, destino.lon) * 100.0d) / 100.0d
    def progresoPct = Math.round(rnd.nextDouble(5.0d, 95.0d) * 100.0d) / 100.0d
    [
        id_camion: "camion_${i}",
        id: "camion_${i}",
        lat: lat,
        lon: lon,
        posicion_actual: [lat: lat, lon: lon],
        ruta: ruta,
        ruta_origen: origen.id,
        ruta_destino: destino.id,
        ruta_sugerida: ruta,
        origen_tramo: origen.id,
        destino_tramo: destino.id,
        nodo_actual: progresoPct < 50.0d ? origen.id : destino.id,
        progreso_pct: progresoPct,
        distancia_total_km: distanciaTotal,
        estado_ruta: "En ruta",
        motivo_retraso: ""
    ]
}

def payload = [
    origen: "simlog_nifi_invokehttp",
    canal_ingesta: "nifi",
    ejecutor_ingesta: "PG_SIMLOG_KDD",
    timestamp: java.time.Instant.now().toString(),
    paso_15min: paso,
    clima_hubs: [:],
    clima: [],
    incidencias_dgt: [],
    resumen_dgt: [source_mode: "disabled", incidencias_totales: 0, nodos_afectados: 0, error: ""],
    estados_nodos: estadosNodos,
    nodos_estado: estadosNodos,
    estados_aristas: aristasEstado,
    aristas_estado: aristasEstado,
    camiones: camiones,
    alertas_operativas: []
]

def json = JsonOutput.prettyPrint(JsonOutput.toJson(payload))
flowFile = session.write(flowFile, { out ->
    out.write(json.getBytes("UTF-8"))
} as OutputStreamCallback)

session.putAttribute(flowFile, "mime.type", "application/json")
session.putAttribute(flowFile, "filename", "simlog_practice_${System.currentTimeMillis()}.json")
session.putAttribute(flowFile, "simlog.provenance.sources", "simulacion")
session.putAttribute(flowFile, "simlog.provenance.stage", "synthetic_payload")
session.putAttribute(flowFile, "simlog.provenance.dgt_mode", "disabled")
session.transfer(flowFile, REL_SUCCESS)
