"""
50 capitales de provincia (España peninsular e islas) — coordenadas aproximadas (WGS84).
Usado por el generador de red del gemelo digital.
"""
from __future__ import annotations

from typing import Any, Dict, List

# id estable: CAP_<slug> — coincide con prefijos en nodos del grafo gemelo
CAPITALES_PROVINCIA_ES: List[Dict[str, Any]] = [
    {"id": "CAP_VITORIA", "nombre": "Vitoria-Gasteiz", "provincia": "Álava", "lat": 42.8467, "lon": -2.6719},
    {"id": "CAP_ALBACETE", "nombre": "Albacete", "provincia": "Albacete", "lat": 38.9943, "lon": -1.8585},
    {"id": "CAP_ALICANTE", "nombre": "Alicante", "provincia": "Alicante", "lat": 38.3452, "lon": -0.4810},
    {"id": "CAP_ALMERIA", "nombre": "Almería", "provincia": "Almería", "lat": 36.8381, "lon": -2.4597},
    {"id": "CAP_OVIEDO", "nombre": "Oviedo", "provincia": "Asturias", "lat": 43.3614, "lon": -5.8593},
    {"id": "CAP_AVILA", "nombre": "Ávila", "provincia": "Ávila", "lat": 40.6561, "lon": -4.6813},
    {"id": "CAP_BADAJOZ", "nombre": "Badajoz", "provincia": "Badajoz", "lat": 38.8794, "lon": -6.9706},
    {"id": "CAP_PALMA", "nombre": "Palma", "provincia": "Baleares", "lat": 39.5696, "lon": 2.6502},
    {"id": "CAP_BARCELONA", "nombre": "Barcelona", "provincia": "Barcelona", "lat": 41.3851, "lon": 2.1734},
    {"id": "CAP_BURGOS", "nombre": "Burgos", "provincia": "Burgos", "lat": 42.3439, "lon": -3.6969},
    {"id": "CAP_CACERES", "nombre": "Cáceres", "provincia": "Cáceres", "lat": 39.4753, "lon": -6.3724},
    {"id": "CAP_CADIZ", "nombre": "Cádiz", "provincia": "Cádiz", "lat": 36.5271, "lon": -6.2886},
    {"id": "CAP_SANTANDER", "nombre": "Santander", "provincia": "Cantabria", "lat": 43.4623, "lon": -3.8099},
    {"id": "CAP_CASTELLON", "nombre": "Castellón de la Plana", "provincia": "Castellón", "lat": 39.9864, "lon": -0.0513},
    {"id": "CAP_CIUDADREAL", "nombre": "Ciudad Real", "provincia": "Ciudad Real", "lat": 38.9848, "lon": -3.9278},
    {"id": "CAP_CORDOBA", "nombre": "Córdoba", "provincia": "Córdoba", "lat": 37.8882, "lon": -4.7794},
    {"id": "CAP_ACORUNA", "nombre": "A Coruña", "provincia": "A Coruña", "lat": 43.3623, "lon": -8.4115},
    {"id": "CAP_CUENCA", "nombre": "Cuenca", "provincia": "Cuenca", "lat": 40.0704, "lon": -2.1374},
    {"id": "CAP_GIRONA", "nombre": "Girona", "provincia": "Girona", "lat": 41.9794, "lon": 2.8214},
    {"id": "CAP_GRANADA", "nombre": "Granada", "provincia": "Granada", "lat": 37.1773, "lon": -3.5986},
    {"id": "CAP_GUADALAJARA", "nombre": "Guadalajara", "provincia": "Guadalajara", "lat": 40.6328, "lon": -3.1672},
    {"id": "CAP_SANSEBASTIAN", "nombre": "San Sebastián", "provincia": "Gipuzkoa", "lat": 43.3183, "lon": -1.9812},
    {"id": "CAP_HUELVA", "nombre": "Huelva", "provincia": "Huelva", "lat": 37.2614, "lon": -6.9447},
    {"id": "CAP_HUESCA", "nombre": "Huesca", "provincia": "Huesca", "lat": 42.1401, "lon": -0.4089},
    {"id": "CAP_JAEN", "nombre": "Jaén", "provincia": "Jaén", "lat": 37.7796, "lon": -3.7849},
    {"id": "CAP_LEON", "nombre": "León", "provincia": "León", "lat": 42.5987, "lon": -5.5671},
    {"id": "CAP_LLEIDA", "nombre": "Lleida", "provincia": "Lleida", "lat": 41.6176, "lon": 0.6200},
    {"id": "CAP_LUGO", "nombre": "Lugo", "provincia": "Lugo", "lat": 43.0097, "lon": -7.5560},
    {"id": "CAP_MADRID", "nombre": "Madrid", "provincia": "Madrid", "lat": 40.4168, "lon": -3.7038},
    {"id": "CAP_MALAGA", "nombre": "Málaga", "provincia": "Málaga", "lat": 36.7213, "lon": -4.4214},
    {"id": "CAP_MURCIA", "nombre": "Murcia", "provincia": "Murcia", "lat": 37.9922, "lon": -1.1307},
    {"id": "CAP_PAMPLONA", "nombre": "Pamplona", "provincia": "Navarra", "lat": 42.8125, "lon": -1.6458},
    {"id": "CAP_OURENSE", "nombre": "Ourense", "provincia": "Ourense", "lat": 42.3358, "lon": -7.8639},
    {"id": "CAP_PALENCIA", "nombre": "Palencia", "provincia": "Palencia", "lat": 42.0095, "lon": -4.5288},
    {"id": "CAP_LASPALMAS", "nombre": "Las Palmas", "provincia": "Las Palmas", "lat": 28.1235, "lon": -15.4363},
    {"id": "CAP_PONTEVEDRA", "nombre": "Pontevedra", "provincia": "Pontevedra", "lat": 42.4296, "lon": -8.6446},
    {"id": "CAP_LOGRONO", "nombre": "Logroño", "provincia": "La Rioja", "lat": 42.4627, "lon": -2.4450},
    {"id": "CAP_SALAMANCA", "nombre": "Salamanca", "provincia": "Salamanca", "lat": 40.9701, "lon": -5.6635},
    {"id": "CAP_SANTACRUZ", "nombre": "Santa Cruz de Tenerife", "provincia": "Santa Cruz de Tenerife", "lat": 28.4636, "lon": -16.2518},
    {"id": "CAP_SEGOVIA", "nombre": "Segovia", "provincia": "Segovia", "lat": 40.9429, "lon": -4.1088},
    {"id": "CAP_SEVILLA", "nombre": "Sevilla", "provincia": "Sevilla", "lat": 37.3891, "lon": -5.9845},
    {"id": "CAP_SORIA", "nombre": "Soria", "provincia": "Soria", "lat": 41.7665, "lon": -2.4790},
    {"id": "CAP_TARRAGONA", "nombre": "Tarragona", "provincia": "Tarragona", "lat": 41.1189, "lon": 1.2445},
    {"id": "CAP_TERUEL", "nombre": "Teruel", "provincia": "Teruel", "lat": 40.3456, "lon": -1.1065},
    {"id": "CAP_TOLEDO", "nombre": "Toledo", "provincia": "Toledo", "lat": 39.8628, "lon": -4.0273},
    {"id": "CAP_VALENCIA", "nombre": "Valencia", "provincia": "Valencia", "lat": 39.4699, "lon": -0.3763},
    {"id": "CAP_VALLADOLID", "nombre": "Valladolid", "provincia": "Valladolid", "lat": 41.6523, "lon": -4.7245},
    {"id": "CAP_BILBAO", "nombre": "Bilbao", "provincia": "Vizcaya", "lat": 43.2630, "lon": -2.9350},
    {"id": "CAP_ZAMORA", "nombre": "Zamora", "provincia": "Zamora", "lat": 41.5033, "lon": -5.7467},
    {"id": "CAP_ZARAGOZA", "nombre": "Zaragoza", "provincia": "Zaragoza", "lat": 41.6488, "lon": -0.8891},
]


def lista_capitales() -> List[Dict[str, Any]]:
    return list(CAPITALES_PROVINCIA_ES)
