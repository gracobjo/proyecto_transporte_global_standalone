/**
 * NiFi ExecuteScript (Groovy) - Mezcla respuesta OpenWeather en payload sintetico.
 *
 * Requiere:
 * - FlowFile content: payload JSON sintetico
 * - Atributo "owm.response": respuesta JSON de OpenWeather Group API
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
def openWeatherOk = statusCode >= 200 && statusCode < 300 && weather instanceof Map && (weather.list instanceof List)

def estadoCarretera = { String descripcion ->
    def desc = (descripcion ?: "").toLowerCase(Locale.ROOT)
    if (desc.contains("niebla") || desc.contains("fog") || desc.contains("mist")) return "Niebla"
    if (desc.contains("lluvia") || desc.contains("rain")) return "Lluvia"
    if (desc.contains("nieve") || desc.contains("snow")) return "Nieve"
    if (desc.contains("tormenta") || desc.contains("storm") || desc.contains("thunder")) return "Tormenta"
    return "Optimo"
}

if (openWeatherOk) {
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
session.putAttribute(flowFile, "simlog.weather.available", String.valueOf(openWeatherOk))
session.putAttribute(flowFile, "simlog.provenance.stage", openWeatherOk ? "weather_merged" : "weather_unavailable")
session.putAttribute(flowFile, "simlog.provenance.sources", openWeatherOk ? "simulacion,openweather" : "simulacion")
session.transfer(flowFile, REL_SUCCESS)
