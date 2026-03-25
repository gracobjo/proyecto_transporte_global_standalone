# Diseño de la UI del ciclo KDD (Streamlit)

Documento de **diseño** de las funcionalidades añadidas en la pestaña **Ciclo KDD** de `app_visualizacion.py` y módulos bajo `servicios/`. Complementa [DISENO_SISTEMA.md](DISENO_SISTEMA.md).

## Objetivos de diseño

1. **Trazabilidad didáctica**: enlazar cada fase KDD con **scripts reales** del repositorio y fragmentos de datos (payload, clima, GPS).
2. **Evitar duplicidad visual y de widgets**: una sola instancia de formularios y gráficos interactivos por sesión (claves Streamlit únicas por contexto).
3. **Separar vistas de la misma red**: **topología lógica** (Altair / NetworkX) frente a **mapa geográfico** (Folium en otras pestañas).

## Módulos y responsabilidades

| Módulo | Responsabilidad |
|--------|-----------------|
| `servicios/kdd_fases.py` | Definición central de fases (`FaseKDD`, `FASES_KDD`). |
| `servicios/kdd_vista_ficheros.py` | Vista previa por líneas de ficheros del repo; panel ingesta fases 1–2 (GPS, OpenWeather, simulación); `widget_scope` para claves únicas. |
| `servicios/kdd_reglas_ui.py` | Markdown unificado de reglas de negocio del grafo (sin repetir bloques comunes entre fases 3–5). |
| `servicios/kdd_vista_grafo.py` | Gráfico Altair de topología (una figura conceptual para fases 3–5). |
| `ingesta/ingesta_kdd.py` | `consulta_clima_hubs(api_key=...)` opcional para pruebas desde el dashboard. |

## Fases 1–2: flujo de UI

- **Tarjeta principal** (`widget_scope=kdd_principal`): slider de **paso temporal** (0–96) sincronizado con `st.session_state.paso_15min` y sidebar; botones **Ejecutar ingesta con este paso** y **Guardar instantánea**; comparación **antes/después** del `ultimo_payload.json`; expanders para script `ingesta/ingesta_kdd.py`, `config_nodos.py` (fase 1), fragmentos de **`camiones`**, formulario **OpenWeather** (API key en sesión, no en disco).
- **Lista «Ver todas las fases»**: `mostrar_vista_previa=False` — solo texto de la fase; evita **duplicar** formularios y el error `StreamlitAPIException` por `key` de `st.form` repetida.

## Fases 3–5: flujo de UI

- **Reglas**: un expander con texto **común** (grafo base + autosanación) y subapartados por fase; indicador **«Estás aquí»** según fase seleccionada.
- **Grafo**: un subtítulo fijo que deja claro que es la **misma** red; título de gráfico estable; leyenda de color (Cassandra) y tamaño (PageRank a partir de datos disponibles).
- **Mapas geográficos**: no se duplican aquí; se remite a **Mapa y métricas** y **Rutas híbridas**.

## Decisiones de UX eliminadas (deuda resuelta)

- Diagrama ASCII duplicado al final de la pestaña KDD (redundante con el `st.info` superior).
- Tres títulos de gráfico distintos para el mismo layout (sustituido por título único).
- Repetición del mismo markdown de reglas al cambiar de fase 3 a 4 o 5 (sustituido por panel unificado).

## Seguridad y privacidad (API OpenWeather)

- La API key introducida en el formulario vive en **session state** del navegador; no se escribe en `.env` ni en repositorio.
- Si el campo está vacío, la consulta en vivo puede usar `API_WEATHER_KEY` del entorno (comportamiento de `consulta_clima_hubs`).
