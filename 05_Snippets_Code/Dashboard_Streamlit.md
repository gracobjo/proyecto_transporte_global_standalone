# Dashboard Streamlit - Visualización

## 📝 Descripción
Dashboard principal de SIMLOG con Streamlit y Folium.

## 💻 Código
```python
import streamlit as st
import folium
from streamlit_folium import st_folium
from cassandra.cluster import Cluster

def cargar_datos_cassandra():
    """Carga datos desde Cassandra."""
    cluster = Cluster(['localhost'])
    session = cluster.connect('simlog')
    
    # Cargar nodos
    nodos = session.execute("SELECT * FROM nodos").all()
    
    # Cargar camiones
    camiones = session.execute("SELECT * FROM camiones").all()
    
    return nodos, camiones

def crear_mapa(nodos, aristas, camiones):
    """Crea mapa con Folium."""
    m = folium.Map(location=[40.0, -4.0], zoom_start=6)
    
    # Añadir nodos
    for nodo in nodos:
        color = "green" if nodo.estado == "OK" else "red"
        folium.CircleMarker(
            location=[nodo.latitud, nodo.longitud],
            radius=10,
            color=color,
            popup=f"{nodo.nombre} - {nodo.estado}"
        ).add_to(m)
    
    return m

def main():
    st.title("SIMLOG - Dashboard de Transporte")
    
    nodos, camiones = cargar_datos_cassandra()
    mapa = crear_mapa(nodos, aristas, camiones)
    
    st_folium(mapa, width=800, height=600)
    
    # Métricas
    col1, col2, col3 = st.columns(3)
    col1.metric("Nodos Activos", len([n for n in nodos if n.estado == "OK"]))
    col2.metric("Camiones en Ruta", len(camiones))
    col3.metric("Alertas", len([n for n in nodos if n.estado != "OK"]))

if __name__ == "__main__":
    main()
```

## 📌 Uso
```bash
streamlit run app_visualizacion.py
```

## 🔧 Requisitos
- streamlit
- folium
- streamlit-folium
- cassandra-driver

## 🏷️ Tags
#streamlit #dashboard #visualizacion
