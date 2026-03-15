"""
Sistema de Gemelo Digital Logístico - España
Configuración de la topología de red (nodos y aristas)
Ruta base: ~/proyecto_transporte_global/
"""

# =============================================================================
# 5 HUBS PRINCIPALES (coordenadas reales España)
# =============================================================================
HUBS = {
    "Madrid": {"lat": 40.4168, "lon": -3.7038, "tipo": "hub"},
    "Barcelona": {"lat": 41.3851, "lon": 2.1734, "tipo": "hub"},
    "Bilbao": {"lat": 43.2630, "lon": -2.9350, "tipo": "hub"},
    "Vigo": {"lat": 42.2406, "lon": -8.7207, "tipo": "hub"},
    "Sevilla": {"lat": 37.3891, "lon": -5.9845, "tipo": "hub"},
}

# =============================================================================
# 25 CIUDADES SECUNDARIAS (5 por cada hub - satélites)
# Coordenadas reales de España
# =============================================================================
SECUNDARIOS = {
    # Satélites de Madrid
    "Toledo": {"lat": 39.8628, "lon": -4.0273, "hub": "Madrid"},
    "Segovia": {"lat": 40.9429, "lon": -4.1088, "hub": "Madrid"},
    "Guadalajara": {"lat": 40.6289, "lon": -3.1614, "hub": "Madrid"},
    "Cuenca": {"lat": 40.0718, "lon": -2.1340, "hub": "Madrid"},
    "Ávila": {"lat": 40.6564, "lon": -4.6814, "hub": "Madrid"},
    # Satélites de Barcelona
    "Tarragona": {"lat": 41.1189, "lon": 1.2445, "hub": "Barcelona"},
    "Girona": {"lat": 41.9794, "lon": 2.8214, "hub": "Barcelona"},
    "Lleida": {"lat": 41.6176, "lon": 0.6200, "hub": "Barcelona"},
    "Manresa": {"lat": 41.7250, "lon": 1.8260, "hub": "Barcelona"},
    "Sabadell": {"lat": 41.5499, "lon": 2.1103, "hub": "Barcelona"},
    # Satélites de Bilbao
    "Santander": {"lat": 43.4647, "lon": -3.8044, "hub": "Bilbao"},
    "Vitoria": {"lat": 42.8467, "lon": -2.6727, "hub": "Bilbao"},
    "San Sebastián": {"lat": 43.3183, "lon": -1.9812, "hub": "Bilbao"},
    "Pamplona": {"lat": 42.8125, "lon": -1.6458, "hub": "Bilbao"},
    "Logroño": {"lat": 42.4627, "lon": -2.4450, "hub": "Bilbao"},
    # Satélites de Vigo
    "A Coruña": {"lat": 43.3614, "lon": -8.4112, "hub": "Vigo"},
    "Santiago": {"lat": 42.8805, "lon": -8.5457, "hub": "Vigo"},
    "Pontevedra": {"lat": 42.4310, "lon": -8.6444, "hub": "Vigo"},
    "Ourense": {"lat": 42.3358, "lon": -7.8639, "hub": "Vigo"},
    "Lugo": {"lat": 43.0097, "lon": -7.5560, "hub": "Vigo"},
    # Satélites de Sevilla
    "Córdoba": {"lat": 37.8882, "lon": -4.7794, "hub": "Sevilla"},
    "Málaga": {"lat": 36.7213, "lon": -4.4214, "hub": "Sevilla"},
    "Cádiz": {"lat": 36.5271, "lon": -6.2886, "hub": "Sevilla"},
    "Huelva": {"lat": 37.2614, "lon": -6.9447, "hub": "Sevilla"},
    "Jerez": {"lat": 36.6850, "lon": -6.1273, "hub": "Sevilla"},
}

# =============================================================================
# CONSTRUCCIÓN DEL DICCIONARIO DE NODOS COMPLETO
# =============================================================================
def get_nodos() -> dict:
    """
    Retorna diccionario con todos los nodos: id -> {lat, lon, tipo, hub?}
    """
    nodos = {}
    for nombre, datos in HUBS.items():
        nodos[nombre] = {
            "lat": datos["lat"],
            "lon": datos["lon"],
            "tipo": "hub",
        }
    for nombre, datos in SECUNDARIOS.items():
        nodos[nombre] = {
            "lat": datos["lat"],
            "lon": datos["lon"],
            "tipo": "secundario",
            "hub": datos["hub"],
        }
    return nodos

# =============================================================================
# ARISTAS: Malla principal (hubs entre sí) + Estrella (secundarios)
# Cada secundario tiene al menos 2 conexiones para evitar aislamiento
# =============================================================================

# Malla principal: todos los hubs conectados entre sí
HUB_NAMES = list(HUBS.keys())
MALLA_PRINCIPAL = [
    ("Madrid", "Barcelona"),
    ("Madrid", "Bilbao"),
    ("Madrid", "Vigo"),
    ("Madrid", "Sevilla"),
    ("Barcelona", "Bilbao"),
    ("Barcelona", "Vigo"),
    ("Barcelona", "Sevilla"),
    ("Bilbao", "Vigo"),
    ("Bilbao", "Sevilla"),
    ("Vigo", "Sevilla"),
]

# Estrella: cada secundario conectado a su hub + conexión con vecino
# Para cumplir "al menos 2 conexiones", cada secundario conecta:
# 1) A su hub
# 2) A otro secundario del mismo hub o hub vecino
CONEXIONES_ESTRELLA = []
for nombre, datos in SECUNDARIOS.items():
    hub = datos["hub"]
    CONEXIONES_ESTRELLA.append((nombre, hub))  # Conexión a su hub

# Conexiones adicionales entre secundarios (2ª conexión mínima para evitar aislamiento)
CONEXIONES_SECUNDARIOS = [
    # Madrid
    ("Toledo", "Cuenca"),
    ("Segovia", "Ávila"),
    ("Guadalajara", "Cuenca"),
    # Barcelona
    ("Tarragona", "Lleida"),
    ("Girona", "Sabadell"),
    ("Manresa", "Lleida"),
    # Bilbao
    ("Santander", "San Sebastián"),
    ("Vitoria", "Pamplona"),
    ("San Sebastián", "Pamplona"),
    ("Logroño", "Vitoria"),
    # Vigo
    ("A Coruña", "Santiago"),
    ("Santiago", "Pontevedra"),
    ("Pontevedra", "Ourense"),
    ("Ourense", "Lugo"),
    # Sevilla
    ("Córdoba", "Málaga"),
    ("Málaga", "Cádiz"),
    ("Huelva", "Jerez"),
    ("Jerez", "Cádiz"),
]


def get_aristas() -> list:
    """
    Retorna lista de aristas: (src, dst, distancia_km)
    Incluye malla principal + estrella + conexiones secundarios.
    """
    import math
    nodos = get_nodos()

    def haversine_km(lat1, lon1, lat2, lon2):
        R = 6371  # Radio Tierra km
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
        return round(2 * R * math.asin(math.sqrt(a)), 2)

    aristas = []
    processed = set()

    def add_edge(a, b):
        key = tuple(sorted([a, b]))
        if key not in processed and a in nodos and b in nodos:
            dist = haversine_km(nodos[a]["lat"], nodos[a]["lon"], nodos[b]["lat"], nodos[b]["lon"])
            aristas.append((a, b, dist))
            processed.add(key)

    for src, dst in MALLA_PRINCIPAL:
        add_edge(src, dst)

    for src, dst in CONEXIONES_ESTRELLA:
        add_edge(src, dst)

    for src, dst in CONEXIONES_SECUNDARIOS:
        add_edge(src, dst)

    return aristas


# Export para uso en otros módulos
RED = {
    "nodos": get_nodos(),
    "aristas": get_aristas(),
    "hubs": list(HUBS.keys()),
    "secundarios": list(SECUNDARIOS.keys()),
}
