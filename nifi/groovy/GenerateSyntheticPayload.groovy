/**
 * NiFi ExecuteScript (Groovy) — GPS sintético + envelope JSON compatible con ingesta Python.
 * Propiedades del procesador ExecuteScript:
 *   Script Engine: Groovy
 *   Script Body: <pegar este archivo>
 */

import groovy.json.JsonOutput
import org.apache.nifi.processor.io.OutputStreamCallback

def flowFile = session.get()
if (!flowFile) return

def rnd = java.util.concurrent.ThreadLocalRandom.current()
def hubs = [
    [id: "Madrid", lat: 40.4168d, lon: -3.7038d],
    [id: "Barcelona", lat: 41.3851d, lon: 2.1734d],
    [id: "Bilbao", lat: 43.2630d, lon: -2.9350d],
    [id: "Vigo", lat: 42.2406d, lon: -8.7207d],
    [id: "Sevilla", lat: 37.3891d, lon: -5.9845d]
]

def pasoStr = flowFile.getAttribute("paso_15min")
def paso = (pasoStr != null && !pasoStr.isEmpty()) ? pasoStr.toInteger() : 0

def camiones = (1..5).collect { i ->
    def h = hubs[rnd.nextInt(hubs.size())]
    def d = hubs[rnd.nextInt(hubs.size())]
    def j1 = (rnd.nextDouble() - 0.5d) * 0.12d
    def j2 = (rnd.nextDouble() - 0.5d) * 0.12d
    def lat = h.lat + j1
    def lon = h.lon + j2
    def cid = "camion_${i}"
    def ruta = [h.id, d.id]
    [
        id_camion: cid,
        lat: lat,
        lon: lon,
        ruta: ruta,
        ruta_origen: h.id,
        ruta_destino: d.id,
        ruta_sugerida: ruta,
        estado_ruta: "En ruta",
        motivo_retraso: null,
        # Compatibilidad con consumidores antiguos
        id: cid,
        posicion_actual: [lat: lat, lon: lon]
    ]
}

def payload = [
    origen: "simlog_nifi_synthetic",
    timestamp: java.time.Instant.now().toString(),
    paso_15min: paso,
    clima_hubs: [:],
    nodos_estado: [:],
    aristas_estado: [],
    camiones: camiones
]

def json = JsonOutput.toJson(payload)

flowFile = session.write(flowFile, { out ->
    out.write(json.getBytes("UTF-8"))
} as OutputStreamCallback)

session.putAttribute(flowFile, "mime.type", "application/json")
session.putAttribute(flowFile, "filename", "synthetic_${System.currentTimeMillis()}.json")
session.transfer(flowFile, REL_SUCCESS)
