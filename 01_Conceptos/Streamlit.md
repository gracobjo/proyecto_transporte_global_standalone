# Streamlit

## 🧠 Definición
Framework Python de código abierto para crear aplicaciones web de datos de forma rápida sin necesidad de HTML/CSS.

## ⚙️ Cómo funciona
- Decoradores Python para definir UI
- Recarga automática en cambios
- Componentes interactivos
- Integración con pandas, matplotlib, folium

## 📊 Uso en SIMLOG
- Dashboard principal (`app_visualizacion.py`)
- Cuadro de mando (`cuadro_mando_ui.py`)
- Panel FAQ IA

## 📊 Ejemplo
```python
import streamlit as st

st.title("SIMLOG Dashboard")
st.map(df_coordenadas)
st.line_chart(datos_tiempo)
```

## 🔗 Relacionado
- [[Folium]]
- [[SIMLOG_Transporte_Global]]

## 🏷️ Tags
#streamlit #dashboard #visualizacion
