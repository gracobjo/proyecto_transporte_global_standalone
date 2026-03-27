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
if (!weatherResponse) {
    flowFile = session.putAttribute(flowFile, "merge.error", "missing owm.response")
    session.transfer(flowFile, REL_FAILURE)
    return
}

def payloadText = ""
flowFile = session.read(flowFile, { inputStream ->
    payloadText = inputStream.getText("UTF-8")
} as InputStreamCallback)

def slurper = new JsonSlurper()
def payload = slurper.parseText(payloadText ?: "{}")
def weather = slurper.parseText(weatherResponse ?: "{}")

def estadoCarretera = { String descripcion ->
    def desc = (descripcion ?: "").toLowerCase(Locale.ROOT)
    if (desc.contains("niebla") || desc.contains("fog") || desc.contains("mist")) return "Niebla"
    if (desc.contains("lluvia") || desc.contains("rain")) return "Lluvia"
    if (desc.contains("nieve") || desc.contains("snow")) return "Nieve"
    if (desc.contains("tormenta") || desc.contains("storm") || desc.contains("thunder")) return "Tormenta"
    return "Optimo"
}

def climaHubs = [:]
def items = weather instanceof Map ? (weather.list ?: []) : []
items.each { w ->
    def ciudad = w?.name ?: "desconocida"
    def descripcion = (w.weather && w.weather[0]) ? String.valueOf(w.weather[0].description) : ""
    climaHubs[ciudad] = [
        descripcion: descripcion,
        temp: w.main?.temp,
        humedad: w.main?.humidity,
        viento: w.wind?.speed,
        visibilidad: w.visibility,
        estado_carretera: estadoCarretera(descripcion)
    ]
}

payload.clima_hubs = climaHubs
payload.clima = climaHubs.collect { ciudad, datos ->
    [
        ciudad: ciudad,
        temperatura: datos.temp,
        humedad: datos.humedad,
        descripcion: datos.descripcion,
        visibilidad: datos.visibilidad,
        estado_carretera: datos.estado_carretera,
        timestamp: payload.timestamp
    ]
}

def finalJson = JsonOutput.prettyPrint(JsonOutput.toJson(payload))
flowFile = session.write(flowFile, { out ->
    out.write(finalJson.getBytes("UTF-8"))
} as OutputStreamCallback)

session.putAttribute(flowFile, "mime.type", "application/json")
session.putAttribute(flowFile, "simlog.provenance.stage", "weather_merged")
session.putAttribute(flowFile, "simlog.provenance.sources", "simulacion,openweather")
session.transfer(flowFile, REL_SUCCESS)
