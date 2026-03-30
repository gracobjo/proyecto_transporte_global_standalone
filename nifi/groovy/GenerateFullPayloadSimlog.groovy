/**
 * NiFi ExecuteScript (Groovy) — Ingesta KDD completa compatible con Spark (`procesamiento_grafos`).
 *
 * Clima: Open-Meteo (una petición multi-hub; sin API key).
 * Estados simulados de nodos/aristas y camiones (alineado con `ingesta/ingesta_kdd.py`).
 *
 * Atributos de entrada (opcional):
 *   - paso_15min   (default 0)
 *
 * Salida: FlowFile JSON en el cuerpo; mime.type application/json
 */

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.nifi.processor.io.OutputStreamCallback

def flowFile = session.get()
if (!flowFile) return

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

def wmoDescEs = { int code ->
    def m = [
        0: "cielo despejado", 1: "mayormente despejado", 2: "parcialmente nublado", 3: "nublado",
        45: "niebla", 48: "niebla con escarcha",
        51: "llovizna ligera", 53: "llovizna moderada", 55: "llovizna densa",
        61: "lluvia ligera", 63: "lluvia moderada", 65: "lluvia fuerte",
        71: "nieve ligera", 73: "nieve moderada", 75: "nieve fuerte",
        80: "chubascos ligeros", 81: "chubascos moderados", 82: "chubascos violentos",
        95: "tormenta", 96: "tormenta con granizo ligero", 99: "tormenta con granizo fuerte"
    ]
    return m[code] ?: "condición WMO ${code}"
}

def lats = hubs.collect { it.lat }.join(",")
def lons = hubs.collect { it.lon }.join(",")
def url = "https://api.open-meteo.com/v1/forecast?latitude=${lats}&longitude=${lons}" +
    "&current=temperature_2m,relative_humidity_2m,weather_code,wind_speed_10m,wind_gusts_10m,visibility,rain,snowfall,cloud_cover" +
    "&timezone=auto&wind_speed_unit=ms"

def clima = [:]
try {
    def conn = new URL(url).openConnection()
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(25000)
    def slurper = new JsonSlurper()
    def arr = slurper.parse(conn.getInputStream())
    if (arr instanceof List) {
        arr.eachWithIndex { loc, idx ->
            if (idx >= hubs.size()) return
            def hid = hubs[idx].id
            def cur = loc?.current
            if (cur == null) {
                clima[hid] = [descripcion: "sin current", temp: null, humedad: null, viento: null, source: "openmeteo"]
                return
            }
            def wmo = (cur.weather_code != null) ? (cur.weather_code as Integer) : 0
            clima[hid] = [
                descripcion: wmoDescEs(wmo),
                temp: cur.temperature_2m,
                humedad: cur.relative_humidity_2m,
                viento: cur.wind_speed_10m,
                source: "openmeteo"
            ]
        }
    }
} catch (Exception e) {
    hubs.each { h ->
        clima[h.id] = [
            descripcion: "Error: ${e.class.simpleName}",
            temp: null,
            humedad: null,
            viento: null,
            source: "openmeteo"
        ]
    }
}

def estados = ["OK", "Congestionado", "Bloqueado"]

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
    def lat = h.lat + j1
    def lon = h.lon + j2
    def cid = "camion_${i}"
    def ruta = [h.id, dest.id]
    [
        id_camion: cid,
        lat: lat,
        lon: lon,
        ruta: ruta,
        ruta_origen: h.id,
        ruta_destino: dest.id,
        ruta_sugerida: ruta,
        posicion_actual: [lat: lat, lon: lon],
        progreso_pct: 25 * (i % 4),
        estado_ruta: "En ruta",
        motivo_retraso: null,
        id: cid
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
