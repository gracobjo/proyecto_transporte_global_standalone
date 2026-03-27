from __future__ import annotations

from ingesta.ingesta_dgt_datex2 import fusionar_estados, mapear_incidencias_a_nodos, parsear_xml_datex2


SAMPLE_DATEX2_XML = """<?xml version="1.0" encoding="UTF-8"?>
<d2LogicalModel xmlns="http://levelC/schema/3/situation">
  <payloadPublication>
    <situation>
      <situationRecord id="DGT-001" version="1">
        <situationRecordCreationTime>2026-03-27T08:00:00Z</situationRecordCreationTime>
        <overallSeverity>high</overallSeverity>
        <validity>
          <validityTimeSpecification>
            <overallStartTime>2026-03-27T08:00:00Z</overallStartTime>
          </validityTimeSpecification>
        </validity>
        <groupOfLocations>
          <locationContainedInGroup>
            <pointByCoordinates>
              <pointCoordinates>
                <latitude>40.4168</latitude>
                <longitude>-3.7038</longitude>
              </pointCoordinates>
            </pointByCoordinates>
          </locationContainedInGroup>
        </groupOfLocations>
        <roadName>A-6</roadName>
        <municipality>Madrid</municipality>
        <province>Madrid</province>
        <generalPublicComment>
          <comment>
            <value>Accidente con corte total</value>
          </comment>
        </generalPublicComment>
      </situationRecord>
    </situation>
  </payloadPublication>
</d2LogicalModel>
"""


def test_parsear_xml_datex2_extrae_campos_clave():
    incidencias = parsear_xml_datex2(SAMPLE_DATEX2_XML)

    assert len(incidencias) == 1
    incidencia = incidencias[0]
    assert incidencia["id_incidencia"] == "DGT-001"
    assert incidencia["severity"] == "high"
    assert incidencia["estado"] == "Bloqueado"
    assert incidencia["carretera"] == "A-6"
    assert incidencia["municipio"] == "Madrid"
    assert incidencia["provincia"] == "Madrid"
    assert incidencia["lat"] == 40.4168
    assert incidencia["lon"] == -3.7038


def test_mapear_y_fusionar_estados_prioriza_dgt_sobre_simulacion():
    incidencias = parsear_xml_datex2(SAMPLE_DATEX2_XML)
    mapeo = mapear_incidencias_a_nodos(incidencias, max_km=100.0)
    simulados = {
        "Madrid": {"estado": "OK", "motivo": None, "source": "simulacion", "severity": "low"},
        "Bilbao": {"estado": "Congestionado", "motivo": "Tráfico", "source": "simulacion", "severity": "medium"},
    }

    merged = fusionar_estados(simulados, mapeo)

    assert merged["Madrid"]["estado"] == "Bloqueado"
    assert merged["Madrid"]["source"] == "dgt"
    assert merged["Madrid"]["severity"] == "high"
    assert merged["Madrid"]["id_incidencia"] == "DGT-001"
    assert merged["Madrid"]["carretera"] == "A-6"
    assert merged["Bilbao"]["estado"] == "Congestionado"
    assert merged["Bilbao"]["source"] == "simulacion"

