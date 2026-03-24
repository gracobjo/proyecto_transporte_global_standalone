/**
 * NiFi ExecuteScript (Groovy) — Ingesta KDD completa compatible con Spark (`procesamiento_grafos`).
 *
 * Incluye:
 * - Clima real OpenWeather para los 5 hubs (HTTP desde Groovy).
 * - Estados simulados de nodos/aristas y camiones (misma idea que `ingesta/ingesta_kdd.py`).
 *
 * Atributos de entrada (desde UpdateAttribute / Parameter Context):
 *   - owm.api.key  (obligatorio) — API key OpenWeather
 *   - paso_15min   (opcional, default 0)
 *
 * Salida: FlowFile JSON en el cuerpo; mime.type application/json
 */

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.nifi.processor.io.OutputStreamCallback

def flowFile = session.get()
if (!flowFile) return

def apiKey = flowFile.getAttribute("owm.api.key")
if (!apiKey) {
    session.putAttribute(flowFile, "error", "missing owm.api.key")
    session.transfer(flowFile, REL_FAILURE)
    return
}

def pasoStr = flowFile.getAttribute("paso_15min")
def paso = (pasoStr != null && !pasoStr.isEmpty()) ? pasoStr.toInteger() : 0

def rnd = new java.util.Random()

def hubs = [
    [id: "Madrid", lat: 40.4168d, lon: -3.7038d],
    [id: "Barcelona", lat: 41.3851d, lon: 2.1734d],
    [id: "Bilbao", lat: 43.2630d, lon: -2.9350d],
    [id: "Vigo", lat: 42.2406d, lon: -8.7207d],
    [id: "Sevilla", lat: 37.3891d, lon: -5.9845d]
]

def estados = ["OK", "Congestionado", "Bloqueado"]

def clima = [:]
hubs.each { h ->
    def url = "https://api.openweathermap.org/data/2.5/weather?lat=${h.lat}&lon=${h.lon}&appid=${apiKey}&units=metric&lang=es"
    try {
        def conn = new URL(url).openConnection()
        conn.setConnectTimeout(15000)
        conn.setReadTimeout(25000)
        def slurper = new JsonSlurper()
        def w = slurper.parse(conn.getInputStream())
        clima[h.id] = [
            descripcion: w.weather ? w.weather[0].description : "N/A",
            temp: w.main?.temp,
            humedad: w.main?.humidity,
            viento: w.wind?.speed
        ]
    } catch (Exception e) {
        clima[h.id] = [
            descripcion: "Error: ${e.class.simpleName}",
            temp: null,
            humedad: null,
            viento: null
        ]
    }
}

def nodos = [:]
hubs.each { h ->
    def e = estados[rnd.nextInt(3)]
    nodos[h.id] = [estado: e, motivo: (e == "OK" ? null : "Simulado NiFi")]
}

def aristas = [:]
(0..12).each {
    def a = hubs[rnd.nextInt(hubs.size())]
    def b = hubs[rnd.nextInt(hubs.size())]
    if (a.id != b.id) {
        def key = "${a.id}|${b.id}"
        def e = estados[rnd.nextInt(3)]
        aristas[key] = [
            estado: e,
            motivo: (e == "OK" ? null : "Sim"),
            distancia_km: 50 + rnd.nextInt(400)
        ]
    }
}

def camiones = (1..5).collect { i ->
    def h = hubs[rnd.nextInt(hubs.size())]
    def dest = hubs[rnd.nextInt(hubs.size())]
    def j1 = (rnd.nextDouble() - 0.5d) * 0.15d
    def j2 = (rnd.nextDouble() - 0.5d) * 0.15d
    [
        id: "camion_${i}",
        lat: h.lat + j1,
        lon: h.lon + j2,
        ruta: [h.id, dest.id],
        posicion_actual: [lat: h.lat + j1, lon: h.lon + j2],
        progreso_pct: 25 * (i % 4),
        estado_ruta: "En ruta",
        motivo_retraso: null
    ]
}

def payload = [
    origen: "simlog_nifi_full",
    timestamp: java.time.Instant.now().toString(),
    paso_15min: paso,
    clima_hubs: clima,
    nodos_estado: nodos,
    aristas_estado: aristas,
    camiones: camiones
]

def json = JsonOutput.toJson(payload)

flowFile = session.write(flowFile, { out ->
    out.write(json.getBytes("UTF-8"))
} as OutputStreamCallback)

session.putAttribute(flowFile, "mime.type", "application/json")
session.putAttribute(flowFile, "filename", "simlog_nifi_${System.currentTimeMillis()}.json")
session.transfer(flowFile, REL_SUCCESS)
