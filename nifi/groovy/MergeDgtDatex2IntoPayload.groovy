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
if (!xmlText) {
    flowFile = session.putAttribute(flowFile, "merge.error", "missing dgt.response.xml")
    session.transfer(flowFile, REL_FAILURE)
    return
}

def payloadText = ""
flowFile = session.read(flowFile, { inputStream ->
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

def localName = { node ->
    def n = node.name()
    if (n instanceof QName) return n.localPart
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

def allRecords = []
def root = new XmlSlurper(false, false).parseText(xmlText)
descendants(root, ["situationRecord"] as Set).each { rec ->
    def severity = (firstText(rec, ["overallSeverity", "severity"]) ?: "low").toLowerCase(Locale.ROOT)
    if (!severityRank.containsKey(severity)) severity = "medium"
    def lat = firstText(rec, ["latitude", "latitudeDegrees"])
    def lon = firstText(rec, ["longitude", "longitudeDegrees"])
    if (!lat || !lon) return
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
        distancia_nodo_km: incident.distancia_nodo_km
    ]
}
payload.estados_nodos = estados
payload.nodos_estado = estados
payload.incidencias_dgt = allRecords
payload.resumen_dgt = [
    source_mode: "live",
    error: "",
    incidencias_totales: allRecords.size(),
    nodos_afectados: affected.size()
]

def finalJson = JsonOutput.prettyPrint(JsonOutput.toJson(payload))
flowFile = session.write(flowFile, { out ->
    out.write(finalJson.getBytes("UTF-8"))
} as OutputStreamCallback)

session.putAttribute(flowFile, "mime.type", "application/json")
session.putAttribute(flowFile, "simlog.provenance.stage", "dgt_merged")
session.putAttribute(flowFile, "simlog.provenance.sources", allRecords ? "simulacion,openweather,dgt" : "simulacion,openweather")
session.putAttribute(flowFile, "simlog.provenance.dgt_mode", allRecords ? "live" : "disabled")
session.putAttribute(flowFile, "simlog.provenance.dgt_incidents", String.valueOf(allRecords.size()))
session.putAttribute(flowFile, "simlog.provenance.dgt_nodes_affected", String.valueOf(affected.size()))
session.transfer(flowFile, REL_SUCCESS)
