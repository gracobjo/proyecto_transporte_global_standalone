/**
 * NiFi ExecuteScript (Groovy) — Mezcla respuesta meteorológica en el payload sintético.
 *
 * Soporta:
 * - Open-Meteo (por defecto en el proyecto): cuerpo JSON = **array** de resultados
 *   (una petición multi-hub: latitude=lat1,lat2,...&longitude=lon1,lon2,...).
 * - OpenWeather (legacy): objeto con `list` (Group API).
 *
 * Requiere:
 * - FlowFile content: payload JSON sintético
 * - Atributo "owm.response": cuerpo JSON de la API (nombre histórico; puede ser Open-Meteo)
 */

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback

def flowFile = session.get()
if (!flowFile) return

def weatherResponse = flowFile.getAttribute("owm.response")
def weatherStatus = flowFile.getAttribute("invokehttp.status.code")

def payloadText = ""
session.read(flowFile, { inputStream ->
    payloadText = inputStream.getText("UTF-8")
} as InputStreamCallback)

def slurper = new JsonSlurper()
def payload = slurper.parseText(payloadText ?: "{}")
def climaHubs = payload.clima_hubs instanceof Map ? payload.clima_hubs : [:]
def climaLista = payload.clima instanceof List ? payload.clima : []

def weather = [:]
if (weatherResponse) {
    try {
        weather = slurper.parseText(weatherResponse ?: "{}")
    } catch (Exception ignored) {
        weather = [:]
    }
}

def statusCode = 0
try {
    statusCode = (weatherStatus ?: "0") as Integer
} catch (Exception ignored) {
    statusCode = 0
}

def httpOk = statusCode >= 200 && statusCode < 300

def estadoCarretera = { String descripcion ->
    def desc = (descripcion ?: "").toLowerCase(Locale.ROOT)
    if (desc.contains("niebla") || desc.contains("fog") || desc.contains("mist")) return "Niebla"
    if (desc.contains("lluvia") || desc.contains("rain")) return "Lluvia"
    if (desc.contains("nieve") || desc.contains("snow")) return "Nieve"
    if (desc.contains("tormenta") || desc.contains("storm") || desc.contains("thunder")) return "Tormenta"
    return "Optimo"
}

/** WMO code (Open-Meteo) → descripción corta ES — alineado con `servicios/open_meteo_clima.py` */
def wmoDescEs = { int code ->
    def m = [
        0: "cielo despejado", 1: "mayormente despejado", 2: "parcialmente nublado", 3: "nublado",
        45: "niebla", 48: "niebla con escarcha",
        51: "llovizna ligera", 53: "llovizna moderada", 55: "llovizna densa",
        56: "llovizna helada ligera", 57: "llovizna helada densa",
        61: "lluvia ligera", 63: "lluvia moderada", 65: "lluvia fuerte",
        66: "lluvia helada ligera", 67: "lluvia helada fuerte",
        71: "nieve ligera", 73: "nieve moderada", 75: "nieve fuerte", 77: "granizo / nieve",
        80: "chubascos ligeros", 81: "chubascos moderados", 82: "chubascos violentos",
        85: "chubascos de nieve ligeros", 86: "chubascos de nieve fuertes",
        95: "tormenta", 96: "tormenta con granizo ligero", 99: "tormenta con granizo fuerte"
    ]
    return m[code] ?: "condición WMO ${code}"
}

def weatherOk = false
def weatherSource = "openmeteo"

if (httpOk && weather instanceof List && weather.size() > 0) {
    // Open-Meteo: array de ubicaciones (mismo orden que lat/lon en la URL)
    def hubNames = ["Madrid", "Barcelona", "Bilbao", "Vigo", "Sevilla"]
    climaHubs = [:]
    weather.eachWithIndex { loc, idx ->
        if (idx >= hubNames.size()) return
        def hub = hubNames[idx]
        def cur = loc?.current
        if (cur == null) return
        def wmo = (cur.weather_code != null) ? (cur.weather_code as Integer) : 0
        def desc = wmoDescEs(wmo)
        def vis = cur.visibility
        if (vis != null) {
            try {
                vis = (vis as Double).round() as Integer
            } catch (Exception ignored) {
                vis = null
            }
        }
        climaHubs[hub] = [
            descripcion: desc,
            temp: cur.temperature_2m,
            humedad: cur.relative_humidity_2m,
            viento: cur.wind_speed_10m,
            visibilidad: vis,
            estado_carretera: estadoCarretera(desc),
            source: "openmeteo",
            fallback_activo: false
        ]
    }
    weatherOk = climaHubs.size() > 0
    weatherSource = "openmeteo"
} else if (httpOk && weather instanceof Map && (weather.list instanceof List)) {
    // OpenWeather Group API (legacy)
    climaHubs = [:]
    def items = weather.list ?: []
    items.each { w ->
        def ciudad = w?.name ?: "desconocida"
        def descripcion = (w.weather && w.weather[0]) ? String.valueOf(w.weather[0].description) : ""
        climaHubs[ciudad] = [
            descripcion: descripcion,
            temp: w.main?.temp,
            humedad: w.main?.humidity,
            viento: w.wind?.speed,
            visibilidad: w.visibility,
            estado_carretera: estadoCarretera(descripcion),
            source: "openweather",
            fallback_activo: false
        ]
    }
    weatherOk = climaHubs.size() > 0
    weatherSource = "openweather"
}

if (weatherOk) {
    climaLista = climaHubs.collect { ciudad, datos ->
        [
            ciudad: ciudad,
            temperatura: datos.temp,
            humedad: datos.humedad,
            descripcion: datos.descripcion,
            visibilidad: datos.visibilidad,
            estado_carretera: datos.estado_carretera,
            source: datos.source,
            fallback_activo: datos.fallback_activo,
            timestamp: payload.timestamp
        ]
    }
}

payload.clima_hubs = climaHubs
payload.clima = climaLista

def finalJson = JsonOutput.prettyPrint(JsonOutput.toJson(payload))
flowFile = session.write(flowFile, { out ->
    out.write(finalJson.getBytes("UTF-8"))
} as OutputStreamCallback)

session.putAttribute(flowFile, "mime.type", "application/json")
session.putAttribute(flowFile, "simlog.weather.available", String.valueOf(weatherOk))
if (weatherOk) {
    session.putAttribute(flowFile, "simlog.weather.source", weatherSource)
}
session.putAttribute(flowFile, "simlog.provenance.stage", weatherOk ? "weather_merged" : "weather_unavailable")
def provSrc = weatherOk ? "simulacion,${weatherSource}" : "simulacion"
session.putAttribute(flowFile, "simlog.provenance.sources", provSrc)
session.transfer(flowFile, REL_SUCCESS)
