/**
 * NiFi ExecuteScript (Groovy) - Mezcla incidencias DATEX2 DGT en el payload.
 *
 * Requiere:
 * - FlowFile content: payload JSON ya enriquecido con OpenWeather.
 * - Atributo "dgt.response.xml": XML DATEX2 v3.x descargado por InvokeHTTP.
 *
 * Añade:
 * - incidencias_dgt
 * - resumen_dgt
 * - source / severity / peso_pagerank en nodos afectados
 * - atributos de provenance para consulta en NiFi UI
 */

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.xml.XmlSlurper
import groovy.xml.slurpersupport.GPathResult
import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback

def flowFile = session.get()
if (!flowFile) return

def xmlText = flowFile.getAttribute("dgt.response.xml")
def dgtUrl = flowFile.getAttribute("dgt.url") ?: "https://nap.dgt.es/datex2/v3/dgt/SituationPublication/datex2_v36.xml"

def payloadText = ""
session.read(flowFile, { inputStream ->
    payloadText = inputStream.getText("UTF-8")
} as InputStreamCallback)

def payload = new JsonSlurper().parseText(payloadText ?: "{}")
def nodos = [
    Madrid: [lat: 40.4168d, lon: -3.7038d],
    Barcelona: [lat: 41.3851d, lon: 2.1734d],
    Bilbao: [lat: 43.2630d, lon: -2.9350d],
    Vigo: [lat: 42.2406d, lon: -8.7207d],
    Sevilla: [lat: 37.3891d, lon: -5.9845d]
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

def severityRank = [low: 1, medium: 2, high: 3, highest: 4]
def severityState = [low: "OK", medium: "Congestionado", high: "Bloqueado", highest: "Bloqueado"]
def severityWeight = [low: 1.0d, medium: 1.5d, high: 3.0d, highest: 3.0d]
def weatherLabels = [
    snowfall: "Nevada",
    snow: "Nieve",
    ice: "Hielo",
    icy: "Hielo",
    frost: "Helada",
    rain: "Lluvia",
    heavyrain: "Lluvia intensa",
    sleet: "Aguanieve",
    hail: "Granizo",
    fog: "Niebla",
    mist: "Niebla",
    wind: "Viento fuerte",
    strongwinds: "Viento fuerte",
    poorenvironment: "Entorno adverso",
    wet: "Mojado",
]
def visibilityMeters = [
    verypoorvisibility: 100,
    poorvisibility: 500,
    reducedvisibility: 1500,
    moderatevisibility: 3000,
    goodvisibility: 10000,
]

def localName = { node ->
    def n = node.name()
    def txt = String.valueOf(n)
    return txt.contains(":") ? txt.split(":")[-1] : txt
}

def descendants = { GPathResult root, Set wanted ->
    root.depthFirst().findAll { child -> wanted.contains(localName(child)) }
}

def firstText = { GPathResult root, List names ->
    def node = descendants(root, names as Set).find { it.text()?.trim() }
    return node ? node.text().trim() : null
}

def allTexts = { GPathResult root, List names ->
    descendants(root, names as Set).collect { it.text()?.trim() }.findAll { it }
}

def cleanToken = { String value ->
    if (!value) return null
    def chars = value.toLowerCase(Locale.ROOT).findAll { Character.isLetterOrDigit((char) it) }
    return chars ? chars.join("") : null
}

def labelToken = { String value ->
    def token = cleanToken(value)
    if (!token) return null
    return weatherLabels[token] ?: value.replace("_", " ").trim().capitalize()
}

def visibilityValue = { String value ->
    def token = cleanToken(value)
    if (!token) return null
    if (visibilityMeters.containsKey(token)) return visibilityMeters[token]
    def digits = value.findAll(/\d/).join("")
    return digits ? Integer.valueOf(digits) : null
}

def inferRoadCondition = { List labels, String explicitValue ->
    if (explicitValue) return explicitValue
    def txt = (labels ?: []).join(" ").toLowerCase(Locale.ROOT)
    if (txt.contains("hielo") || txt.contains("helada")) return "Hielo"
    if (txt.contains("nieve") || txt.contains("aguanieve")) return "Nieve"
    if (txt.contains("lluvia") || txt.contains("granizo")) return "Mojado"
    if (txt.contains("niebla")) return "Visibilidad reducida"
    if (txt.contains("viento")) return "Viento fuerte"
    return null
}

def hasWeather = { Map incident ->
    (incident?.estado_carretera != null) || (incident?.visibilidad != null) || ((incident?.condiciones_meteorologicas ?: []).size() > 0)
}

def parseRoot = { String xml ->
    if (!xml?.trim()) return null
    try {
        return new XmlSlurper(false, false).parseText(xml)
    } catch (Exception ignored) {
        return null
    }
}

def downloadXml = { String url ->
    def conn = new URL(url).openConnection()
    conn.setConnectTimeout(15000)
    conn.setReadTimeout(30000)
    conn.setRequestProperty("Accept", "application/xml,text/xml,*/*")
    conn.setRequestProperty("User-Agent", "SIMLOG-NiFi-DGT/1.0")
    return conn.getInputStream().getText("UTF-8")
}

def root = parseRoot(xmlText)
def xmlSource = "attribute"
if (root == null) {
    try {
        xmlText = downloadXml(dgtUrl)
        root = parseRoot(xmlText)
        xmlSource = "refetch"
    } catch (Exception e) {
        flowFile = session.putAttribute(flowFile, "merge.error", "dgt xml unavailable: ${e.message}")
        session.transfer(flowFile, REL_FAILURE)
        return
    }
}
if (root == null) {
    flowFile = session.putAttribute(flowFile, "merge.error", "dgt xml invalid after refetch")
    session.transfer(flowFile, REL_FAILURE)
    return
}

def allRecords = []
descendants(root, ["situationRecord"] as Set).each { rec ->
    def severity = (firstText(rec, ["overallSeverity", "severity"]) ?: "low").toLowerCase(Locale.ROOT)
    if (!severityRank.containsKey(severity)) severity = "medium"
    def lat = firstText(rec, ["latitude", "latitudeDegrees"])
    def lon = firstText(rec, ["longitude", "longitudeDegrees"])
    if (!lat || !lon) return
    def meteoRaw = allTexts(rec, [
        "poorEnvironmentType",
        "weatherRelatedRoadConditions",
        "weatherRelatedConditionType",
        "roadSurfaceCondition",
        "visibility",
        "visibilityType",
        "precipitationType",
        "precipitationDetail",
    ])
    def meteoLabels = []
    meteoRaw.each { raw ->
        def label = labelToken(raw as String)
        if (label && !meteoLabels.contains(label)) meteoLabels << label
    }
    def roadSurface = labelToken(firstText(rec, ["roadSurfaceCondition", "surfaceCondition", "roadSurfaceConditions"]))
    def visibility = visibilityValue(firstText(rec, ["visibility", "visibilityType"]))
    def incident = [
        id_incidencia: (rec.@id?.text() ?: "dgt-${System.nanoTime()}"),
        source: "dgt",
        severity: severity,
        estado: severityState[severity],
        peso_pagerank: severityWeight[severity],
        descripcion: firstText(rec, ["value", "comment", "description"]) ?: "Incidencia DGT",
        carretera: firstText(rec, ["roadName", "roadNumber", "roadIdentifier"]),
        municipio: firstText(rec, ["municipality", "town", "city", "localityName"]),
        provincia: firstText(rec, ["province", "administrativeArea", "county"]),
        condiciones_meteorologicas: meteoLabels,
        estado_carretera: inferRoadCondition(meteoLabels, roadSurface),
        visibilidad: visibility,
        lat: lat as Double,
        lon: lon as Double,
    ]
    allRecords << incident
}

def affected = [:]
allRecords.each { incident ->
    def best = null
    def bestDist = null
    nodos.each { nid, meta ->
        def dist = haversineKm(incident.lat as Double, incident.lon as Double, meta.lat as Double, meta.lon as Double)
        if (bestDist == null || dist < bestDist) {
            best = nid
            bestDist = dist
        }
    }
    if (best == null || bestDist == null || bestDist > 100.0d) return
    incident.distancia_nodo_km = Math.round(bestDist * 100.0d) / 100.0d
    incident.nodo_cercano = best
    def current = affected[best]
    if (current == null) {
        affected[best] = incident
        return
    }
    def candRank = severityRank[incident.severity] ?: 0
    def curRank = severityRank[current.severity] ?: 0
    if (candRank > curRank || (candRank == curRank && incident.distancia_nodo_km < current.distancia_nodo_km)) {
        affected[best] = incident
    }
}

def estados = payload.estados_nodos instanceof Map ? payload.estados_nodos : [:]
affected.each { nid, incident ->
    def base = estados[nid] instanceof Map ? estados[nid] : [:]
    estados[nid] = base + [
        estado: incident.estado,
        motivo: incident.descripcion,
        source: "dgt",
        severity: incident.severity,
        peso_pagerank: incident.peso_pagerank,
        id_incidencia: incident.id_incidencia,
        carretera: incident.carretera,
        municipio: incident.municipio,
        provincia: incident.provincia,
        descripcion: incident.descripcion,
        distancia_nodo_km: incident.distancia_nodo_km,
        condiciones_meteorologicas: incident.condiciones_meteorologicas ?: [],
        estado_carretera: incident.estado_carretera,
        visibilidad: incident.visibilidad
    ]
}
payload.estados_nodos = estados
payload.nodos_estado = estados
payload.incidencias_dgt = allRecords

def climaHubs = payload.clima_hubs instanceof Map ? payload.clima_hubs : [:]
def weatherAvailable = String.valueOf(flowFile.getAttribute("simlog.weather.available") ?: "false").toBoolean()
if (!weatherAvailable || climaHubs.isEmpty()) {
    def bestWeather = [:]
    allRecords.findAll { hasWeather(it as Map) }.each { incident ->
        nodos.each { hubId, meta ->
            def dist = haversineKm(incident.lat as Double, incident.lon as Double, meta.lat as Double, meta.lon as Double)
            if (dist > 100.0d) return
            def current = bestWeather[hubId]
            def candidateScore = [(severityRank[incident.severity] ?: 0), -dist]
            def currentScore = current ? [(severityRank[current.severity] ?: 0), -(current.distancia_referencia_km as Double)] : null
            if (current == null || candidateScore > currentScore) {
                def resumen = (incident.condiciones_meteorologicas ?: []) ? incident.condiciones_meteorologicas[0] : (incident.estado_carretera ?: "Condiciones adversas")
                def detalle = "Fallback DGT: ${resumen}"
                if (incident.carretera) detalle += " en ${incident.carretera}"
                bestWeather[hubId] = [
                    descripcion: detalle,
                    temp: null,
                    humedad: null,
                    viento: null,
                    visibilidad: incident.visibilidad,
                    estado_carretera: incident.estado_carretera ?: "Comprometido",
                    condiciones_meteorologicas: incident.condiciones_meteorologicas ?: [],
                    source: "dgt",
                    fallback_activo: true,
                    id_incidencia: incident.id_incidencia,
                    distancia_referencia_km: Math.round(dist * 100.0d) / 100.0d,
                    severity: incident.severity,
                ]
            }
        }
    }
    climaHubs = bestWeather
    payload.clima_hubs = climaHubs
    payload.clima = climaHubs.collect { ciudad, datos ->
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
payload.resumen_dgt = [
    source_mode: "live",
    error: "",
    incidencias_totales: allRecords.size(),
    nodos_afectados: affected.size(),
    hubs_clima_respaldo: (payload.clima_hubs instanceof Map ? payload.clima_hubs.size() : 0)
]

def finalJson = JsonOutput.prettyPrint(JsonOutput.toJson(payload))
flowFile = session.write(flowFile, { out ->
    out.write(finalJson.getBytes("UTF-8"))
} as OutputStreamCallback)

session.putAttribute(flowFile, "mime.type", "application/json")
session.putAttribute(flowFile, "simlog.provenance.stage", "dgt_merged")
session.putAttribute(flowFile, "simlog.provenance.sources", allRecords ? (weatherAvailable ? "simulacion,openweather,dgt" : "simulacion,dgt") : (weatherAvailable ? "simulacion,openweather" : "simulacion"))
session.putAttribute(flowFile, "simlog.provenance.dgt_mode", allRecords ? "live" : "disabled")
session.putAttribute(flowFile, "simlog.provenance.dgt_xml_source", xmlSource)
session.putAttribute(flowFile, "simlog.provenance.dgt_incidents", String.valueOf(allRecords.size()))
session.putAttribute(flowFile, "simlog.provenance.dgt_nodes_affected", String.valueOf(affected.size()))
session.transfer(flowFile, REL_SUCCESS)
