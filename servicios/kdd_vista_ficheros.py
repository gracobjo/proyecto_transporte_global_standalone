"""
Vista previa en Streamlit: primeras y últimas líneas de ficheros del repo citados en cada fase KDD.
Fases 1–2: además, GPS sintético (`camiones`) y clima por hubs (`clima_hubs`) desde el último payload.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st

from servicios.kdd_fases import FaseKDD, etiqueta_proveedor_clima_ui

N_LINEAS = 5
MAX_BYTES_LECTURA = 2 * 1024 * 1024  # evita cargar ficheros enormes en memoria
RUTA_SCRIPT_INGESTA = "ingesta/ingesta_kdd.py"


def _lang_por_sufijo(path: Path) -> str:
    ext = path.suffix.lower()
    return {
        ".py": "python",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".json": "json",
        ".hql": "sql",
        ".sql": "sql",
        ".md": "markdown",
        ".sh": "bash",
    }.get(ext, "text")


def _lineas_con_numero(lines: List[str], start: int = 1) -> str:
    w = max(4, len(str(start + len(lines))))
    return "\n".join(f"{start + i:{w}d} │ {line}" for i, line in enumerate(lines))


def preview_primeras_ultimas_lineas(
    path: Path, n: int = N_LINEAS
) -> Tuple[str | None, str | None, str | None]:
    """
    Devuelve (bloque_cabecera, bloque_pie, mensaje_error).
    Si el fichero cabe en pocas líneas, solo cabecera (pie y error None).
    """
    try:
        size = path.stat().st_size
    except OSError as e:
        return None, None, str(e)

    if size > MAX_BYTES_LECTURA:
        return (
            None,
            None,
            f"Fichero demasiado grande ({size // (1024 * 1024)} MiB); límite de vista previa {MAX_BYTES_LECTURA // (1024 * 1024)} MiB.",
        )

    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as e:
        return None, None, str(e)

    lines = text.splitlines()
    total = len(lines)
    if total <= 2 * n:
        return _lineas_con_numero(lines), None, None

    head = _lineas_con_numero(lines[:n], start=1)
    tail_start = total - n + 1
    tail = _lineas_con_numero(lines[-n:], start=tail_start)
    omitidas = total - 2 * n
    separador = f"… ({omitidas} línea(s) omitidas) …"
    return head, f"{separador}\n{tail}", None


def _normaliza_token_ruta(raw: str) -> str:
    return str(raw).strip().strip("`").replace("\\", "/")


def _resolver_ficheros_fase(
    base: Path, fase: FaseKDD, omitir_relativas: Tuple[str, ...] = ()
) -> List[Path]:
    """Rutas existentes bajo `base`, deduplicadas, a partir de `stack` + `script`."""
    base_r = base.resolve()
    omit = {_normaliza_token_ruta(x) for x in omitir_relativas}
    ordered: List[Path] = []
    seen: set[str] = set()
    for raw in (*fase.stack, fase.script):
        if not raw or not str(raw).strip():
            continue
        s = _normaliza_token_ruta(str(raw))
        if s in omit:
            continue
        candidate = (base_r / s).resolve()
        try:
            candidate.relative_to(base_r)
        except ValueError:
            continue
        key = str(candidate)
        if candidate.is_file() and key not in seen:
            seen.add(key)
            ordered.append(candidate)
    return ordered


def _ruta_payload_ingesta(base: Path) -> Path:
    return base.resolve() / "reports" / "kdd" / "work" / "ultimo_payload.json"


def _cargar_payload_ingesta(base: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    p = _ruta_payload_ingesta(base)
    if not p.is_file():
        return None, f"Aún no existe `{p.relative_to(base.resolve())}`. Ejecuta la ingesta."
    try:
        return json.loads(p.read_text(encoding="utf-8")), None
    except (OSError, json.JSONDecodeError) as e:
        return None, str(e)


def _estado_normalizado(est: Any) -> str:
    return str(est or "OK").strip().lower()


def _conteo_por_estado_mapa(estados_map: Optional[Dict[str, Any]]) -> Dict[str, int]:
    c = {"ok": 0, "congestionado": 0, "bloqueado": 0, "otros": 0}
    if not isinstance(estados_map, dict):
        return c
    for v in estados_map.values():
        if not isinstance(v, dict):
            continue
        e = _estado_normalizado(v.get("estado", "OK"))
        if "bloque" in e:
            c["bloqueado"] += 1
        elif "congest" in e:
            c["congestionado"] += 1
        elif e == "ok":
            c["ok"] += 1
        else:
            c["otros"] += 1
    return c


def _markdown_resumen_payload(payload: Dict[str, Any]) -> str:
    ne = _conteo_por_estado_mapa(payload.get("nodos_estado"))
    ae = _conteo_por_estado_mapa(payload.get("aristas_estado"))
    cam = payload.get("camiones") or []
    n_cam = len(cam) if isinstance(cam, list) else 0
    ch = payload.get("clima_hubs") or {}
    n_hub = len(ch) if isinstance(ch, dict) else 0
    return (
        f"- **Timestamp:** `{payload.get('timestamp', '—')}` · **Paso:** `{payload.get('paso_15min', '—')}`\n"
        f"- **Nodos** — OK: **{ne['ok']}**, Congestionado: **{ne['congestionado']}**, "
        f"Bloqueado: **{ne['bloqueado']}**"
        + (f", otros: {ne['otros']}" if ne["otros"] else "")
        + "\n"
        f"- **Aristas** — OK: **{ae['ok']}**, Congestionado: **{ae['congestionado']}**, "
        f"Bloqueado: **{ae['bloqueado']}**"
        + (f", otros: {ae['otros']}" if ae["otros"] else "")
        + "\n"
        f"- **Camiones (GPS simulado):** **{n_cam}** · **Hubs con clima:** **{n_hub}**"
    )


def _diff_payloads_markdown(prev: Optional[Dict[str, Any]], new: Optional[Dict[str, Any]]) -> str:
    if not prev or not new:
        return "_No hay dos cargas para comparar (ejecuta ingesta o guarda una instantánea)._"
    bloques: List[str] = []
    if prev.get("paso_15min") != new.get("paso_15min"):
        bloques.append(
            f"- **Paso simulación:** `{prev.get('paso_15min')}` → **`{new.get('paso_15min')}`**"
        )
    if prev.get("timestamp") != new.get("timestamp"):
        bloques.append(
            f"- **Timestamp payload:** `{prev.get('timestamp')}` → `{new.get('timestamp')}`"
        )
    pn, nn = _conteo_por_estado_mapa(prev.get("nodos_estado")), _conteo_por_estado_mapa(
        new.get("nodos_estado")
    )
    if pn != nn:
        bloques.append(
            f"- **Nodos (OK / Cong. / Bloq.):** "
            f"{pn['ok']}/{pn['congestionado']}/{pn['bloqueado']} → "
            f"**{nn['ok']}/{nn['congestionado']}/{nn['bloqueado']}**"
        )
    pa, na = _conteo_por_estado_mapa(prev.get("aristas_estado")), _conteo_por_estado_mapa(
        new.get("aristas_estado")
    )
    if pa != na:
        bloques.append(
            f"- **Aristas (OK / Cong. / Bloq.):** "
            f"{pa['ok']}/{pa['congestionado']}/{pa['bloqueado']} → "
            f"**{na['ok']}/{na['congestionado']}/{na['bloqueado']}**"
        )
    prev_c = {
        c.get("id_camion"): c
        for c in (prev.get("camiones") or [])
        if isinstance(c, dict) and c.get("id_camion")
    }
    new_c = {
        c.get("id_camion"): c
        for c in (new.get("camiones") or [])
        if isinstance(c, dict) and c.get("id_camion")
    }
    cambios_cam: List[str] = []
    for cid in sorted(set(prev_c) | set(new_c)):
        a, b = prev_c.get(cid), new_c.get(cid)
        if not a or not b:
            cambios_cam.append(f"  - `{cid}`: apareció o desapareció en la lista.")
            continue
        tups = []
        for campo in ("lat", "lon", "indice_tramo", "progreso", "origen_tramo", "destino_tramo"):
            if a.get(campo) != b.get(campo):
                tups.append(f"{campo} {a.get(campo)}→{b.get(campo)}")
        if tups:
            cambios_cam.append(f"  - **`{cid}`:** " + "; ".join(tups))
    if cambios_cam:
        bloques.append("- **Camiones (GPS / tramo):**\n" + "\n".join(cambios_cam[:12]))
        if len(cambios_cam) > 12:
            bloques.append(f"  - _…y {len(cambios_cam) - 12} cambio(s) más._")

    pch, nch = prev.get("clima_hubs") or {}, new.get("clima_hubs") or {}
    if isinstance(pch, dict) and isinstance(nch, dict):
        clim_lines: List[str] = []
        for hub in sorted(set(pch.keys()) | set(nch.keys())):
            a, b = pch.get(hub), nch.get(hub)
            ta = (a or {}).get("temp") if isinstance(a, dict) else None
            tb = (b or {}).get("temp") if isinstance(b, dict) else None
            da = (a or {}).get("descripcion") if isinstance(a, dict) else None
            db = (b or {}).get("descripcion") if isinstance(b, dict) else None
            if ta != tb or da != db:
                clim_lines.append(
                    f"  - `{hub}`: {da} ({ta}°C) → **{db} ({tb}°C)**"
                )
        if clim_lines:
            bloques.append("- **Clima por hub:**\n" + "\n".join(clim_lines))

    if not bloques:
        return "_Misma foto resumida (paso e incidentes agregados iguales). Si cambió solo el texto del timestamp, ya consta arriba._"
    return "\n".join(bloques)


def _render_panel_simulacion_ventana(st, base: Path, widget_scope: str) -> None:
    """Slider de paso, ingesta contextual y comparación antes/después (solo tarjeta principal)."""
    if widget_scope != "kdd_principal":
        st.caption(
            "**Simulación:** el paso temporal, incidentes y GPS se controlan desde la tarjeta principal "
            "de la fase seleccionada o desde el **sidebar** («Línea temporal»)."
        )
        return

    from servicios.ejecucion_pipeline import ejecutar_ingesta

    ws = widget_scope
    st.markdown("##### Simulación por ventana (15 min) e instantáneas")
    st.caption(
        "Cada **paso** entero y el **día** fijan la semilla (`ingesta.trigger_paso.semilla_simulacion`): "
        "cambian incidentes simulados y la posición GPS en ruta. Cada ingesta consulta de nuevo la **API de clima** "
        f"({etiqueta_proveedor_clima_ui()})."
    )

    try:
        _p0 = int(st.session_state.get("paso_15min", 0))
    except (TypeError, ValueError):
        _p0 = 0
    if _p0 < 0 or _p0 > 96:
        _p0 = max(0, min(96, _p0))
        st.session_state.paso_15min = _p0
    paso_ui = st.slider(
        "Paso temporal (arrastra para provocar otra simulación)",
        min_value=0,
        max_value=96,
        value=_p0,
        step=1,
        help="Se sincroniza con `paso_15min` del sidebar: este control lo sobrescribe al mover el slider.",
    )
    st.session_state.paso_15min = int(paso_ui)

    auto = bool(st.session_state.get("ingesta_paso_automatico"))
    if auto:
        st.info(
            "Tienes **paso automático** (reloj) en el sidebar: la ingesta desde aquí usará la ventana UTC actual, "
            "no el valor del slider. Desmarca el checkbox para fijar **PASO_15MIN** manualmente."
        )

    c1, c2 = st.columns(2)
    with c1:
        run_ingesta = st.button(
            "Ejecutar ingesta con este paso",
            type="primary",
            key=f"{ws}_ingesta_desde_fase_btn",
            disabled=auto,
        )
    with c2:
        snap_only = st.button(
            "Guardar instantánea del payload actual (memoria)",
            key=f"{ws}_snap_payload_btn",
            help="Captura lo que hay en disco ahora; luego cambia el paso, ejecuta ingesta y abre «Cambios».",
        )

    if snap_only:
        snap, err = _cargar_payload_ingesta(base)
        if snap:
            st.session_state[f"{ws}_snap_pre_ingesta"] = snap
            st.session_state[f"{ws}_snap_saved_flag"] = True
        else:
            st.session_state[f"{ws}_snap_saved_err"] = err or "Sin payload en disco."
        st.rerun()

    if st.session_state.pop(f"{ws}_snap_saved_flag", None):
        st.success(
            "Instantánea guardada en sesión. Cambia el paso y ejecuta ingesta, o revisa la comparación si ya hay datos nuevos."
        )
    err_snap = st.session_state.pop(f"{ws}_snap_saved_err", None)
    if err_snap:
        st.warning(str(err_snap))

    if run_ingesta:
        prev, _ = _cargar_payload_ingesta(base)
        st.session_state[f"{ws}_snap_pre_ingesta"] = prev
        with st.spinner("Ingesta: clima, incidentes, GPS, Kafka, HDFS…"):
            code, out, err = ejecutar_ingesta(
                None if auto else int(st.session_state.paso_15min),
                simular_incidencias=bool(st.session_state.get("simlog_simular_incidencias", True)),
            )
        st.session_state[f"{ws}_last_ingesta_code"] = code
        if code != 0:
            st.session_state[f"{ws}_last_ingesta_err"] = (err or out or "")[-3500:]
        else:
            st.session_state.pop(f"{ws}_last_ingesta_err", None)
        st.rerun()

    lc = st.session_state.get(f"{ws}_last_ingesta_code")
    if lc is not None:
        if lc != 0:
            st.error(f"Última ingesta lanzada desde esta sección: código **{lc}**.")
            st.code(st.session_state.get(f"{ws}_last_ingesta_err", ""), language="text")
        else:
            st.success("Última ingesta desde esta sección terminó correctamente.")

    pay_cur, err_cur = _cargar_payload_ingesta(base)
    if pay_cur:
        with st.expander(
            "Datos obtenidos en esta fase (resumen del último `ultimo_payload.json`)",
            expanded=True,
            key=f"{ws}_exp_resumen_payload",
        ):
            st.markdown(_markdown_resumen_payload(pay_cur))
    elif err_cur:
        st.caption(err_cur)

    snap = st.session_state.get(f"{ws}_snap_pre_ingesta")
    if snap and pay_cur:
        with st.expander(
            "Cómo ha cambiado respecto a la instantánea previa",
            expanded=True,
            key=f"{ws}_exp_diff_payload",
        ):
            st.caption(
                "Compara la captura **antes** de la última ingesta desde aquí (o la que guardaste a mano) "
                "con el fichero **actual** en disco."
            )
            st.markdown(_diff_payloads_markdown(snap, pay_cur))
    elif pay_cur and not snap:
        st.caption(
            "Tras la primera ingesta desde aquí verás la **comparación** automática; "
            "o usa **Guardar instantánea** antes de cambiar el paso."
        )


def _preview_lineas_desde_lista(items: List[Any], n: int = N_LINEAS) -> Tuple[str, Optional[str]]:
    """Cada elemento en una línea JSON compacta; primeras n y últimas n con numeración."""
    lines = [json.dumps(x, ensure_ascii=False, default=str) for x in items]
    total = len(lines)
    if total == 0:
        return "(vacío)", None
    if total <= 2 * n:
        return _lineas_con_numero(lines), None
    head = _lineas_con_numero(lines[:n], start=1)
    tail_start = total - n + 1
    tail = _lineas_con_numero(lines[-n:], start=tail_start)
    omitidas = total - 2 * n
    return head, f"… ({omitidas} línea(s) omitidas) …\n{tail}"


def _md_celda_txt(val: Any) -> str:
    s = "—" if val is None else str(val)
    return s.replace("|", "·").replace("\n", " ").strip() or "—"


def _tabla_clima_markdown(clima: Dict[str, Any]) -> str:
    filas = []
    for hub, d in sorted(clima.items()):
        hub_s = _md_celda_txt(hub).replace("`", "")
        if not isinstance(d, dict):
            filas.append(f"| `{hub_s}` | — | — | — | — |")
            continue
        filas.append(
            "| `{0}` | {1} | {2} | {3} | {4} |".format(
                hub_s,
                _md_celda_txt(d.get("descripcion", "—")),
                _md_celda_txt(d.get("temp")),
                _md_celda_txt(d.get("humedad")),
                _md_celda_txt(d.get("viento")),
            )
        )
    cuerpo = "\n".join(filas)
    return (
        "| Hub | Descripción | Temp. °C | Humedad % | Viento m/s |\n"
        "|-----|-------------|---------|------------|------------|\n" + cuerpo
    )


def render_vista_previa_fase_ingesta_1_2(st, fase: FaseKDD, base: Path, widget_scope: str) -> None:
    """
    Fases selección / preprocesamiento: un solo preview del script de ingesta,
    más `camiones` (GPS sintético) y `clima_hubs` (Open-Meteo u OpenWeather según config) del último payload.

    `widget_scope` debe ser único por cada sitio que renderiza la misma fase
    (p. ej. tarjeta principal vs. lista «Ver todas las fases») para claves Streamlit.
    """
    orden = fase.orden
    ws = widget_scope
    st.markdown(f"**Script, GPS sintético y clima ({etiqueta_proveedor_clima_ui()})**")
    st.caption(
        "El script `ingesta/ingesta_kdd.py` consulta la API, simula la red e interpola posiciones; "
        "los fragmentos siguientes salen del último JSON guardado en `reports/kdd/work/`."
    )
    _render_panel_simulacion_ventana(st, base, widget_scope)
    st.divider()

    # Otros ficheros del stack (p. ej. config_nodos en fase 1), sin repetir el script de ingesta
    otros = _resolver_ficheros_fase(base, fase, omitir_relativas=(RUTA_SCRIPT_INGESTA,))
    for idx, p in enumerate(otros):
        rel = p.relative_to(base.resolve())
        head, tail, err = preview_primeras_ultimas_lineas(p)
        rel_key = str(rel).replace("/", "_").replace(" ", "_")
        with st.expander(f"`{rel}`", expanded=False, key=f"{ws}_vf_ing_{orden}_repo_{idx}_{rel_key}"):
            if err:
                st.warning(err)
                continue
            lang = _lang_por_sufijo(p)
            st.caption("Primeras 5 líneas")
            st.code(head or "", language=lang)
            if tail:
                st.caption("Últimas 5 líneas")
                st.code(tail, language=lang)

    p_script = (base.resolve() / RUTA_SCRIPT_INGESTA).resolve()
    if p_script.is_file():
        head, tail, err = preview_primeras_ultimas_lineas(p_script)
        with st.expander(
            f"Script `ingesta/ingesta_kdd.py` (clima API + simulación + GPS + Kafka/HDFS)",
            expanded=False,
            key=f"{ws}_vf_ing_{orden}_script",
        ):
            if err:
                st.warning(err)
            else:
                st.caption("Primeras 5 líneas")
                st.code(head or "", language="python")
                if tail:
                    st.caption("Últimas 5 líneas")
                    st.code(tail, language="python")

    payload, err_pay = _cargar_payload_ingesta(base)
    rel_pay = _ruta_payload_ingesta(base).relative_to(base.resolve())

    with st.expander(
        "Datos sintéticos de GPS — array `camiones` (una línea por camión)",
        expanded=False,
        key=f"{ws}_vf_ing_{orden}_gps",
    ):
        if err_pay:
            st.info(err_pay)
        else:
            camiones = payload.get("camiones") if payload else None
            if not isinstance(camiones, list):
                st.warning("El payload no contiene una lista `camiones`.")
            else:
                st.caption(f"Fuente: `{rel_pay}` · **{len(camiones)}** registro(s)")
                h, t = _preview_lineas_desde_lista(camiones)
                st.caption("Primeras 5 líneas (JSON compacto por camión)")
                st.code(h, language="json")
                if t:
                    st.caption("Últimas 5 líneas")
                    st.code(t, language="json")

    with st.expander(
        "Clima por nodo — campo `clima_hubs` (toda la red o solo hubs según `SIMLOG_CLIMA_TODOS_NODOS`; última ingesta y consulta en vivo)",
        expanded=False,
        key=f"{ws}_vf_ing_{orden}_clima",
    ):
        if err_pay and not payload:
            st.info(err_pay)
        elif payload and isinstance(payload.get("clima_hubs"), dict):
            st.caption(f"**Última ingesta** (`{rel_pay}`) — timestamp payload: `{payload.get('timestamp', '—')}`")
            st.markdown(_tabla_clima_markdown(payload["clima_hubs"]))
        else:
            st.caption("Sin `clima_hubs` en el último payload.")

        st.divider()
        st.markdown(
            "**Consulta en vivo** — por defecto el proyecto usa **Open-Meteo** (sin clave). "
            "Si configuras `SIMLOG_WEATHER_PROVIDER=openweather`, introduce tu API Key ([OpenWeather](https://openweathermap.org/api)); "
            "se envía como `appid`. La clave queda en la **sesión del navegador**. "
            "Si el campo está **vacío** y usas OpenWeather, se usa `API_WEATHER_KEY` del entorno / `.env`, si existe."
        )
        sk_api = f"{ws}_vf_owm_api_key_f{orden}"
        sk_res = f"{ws}_vf_owm_live_result_f{orden}"
        if sk_api not in st.session_state:
            st.session_state[sk_api] = ""
        with st.form(key=f"{ws}_vf_owm_form_f{orden}", clear_on_submit=False):
            st.text_input(
                "API Key OpenWeather (appid), solo si SIMLOG_WEATHER_PROVIDER=openweather",
                type="password",
                placeholder="Opcional si ya tienes API_WEATHER_KEY configurada",
                help="Tras enviar el formulario, la clave permanece en este campo durante la sesión.",
                key=sk_api,
            )
            submitted = st.form_submit_button("Consultar clima actual")
        if submitted:
            key_explicita = (st.session_state.get(sk_api) or "").strip()
            with st.spinner("Llamando a la API…"):
                try:
                    from ingesta.ingesta_kdd import consulta_clima_hubs

                    live = consulta_clima_hubs(api_key=key_explicita if key_explicita else None)
                except Exception as e:
                    st.session_state[sk_res] = None
                    st.error(f"No se pudo consultar la API: {e}")
                else:
                    st.session_state[sk_res] = live
                    st.success("Respuesta recibida (no se ha modificado `ultimo_payload.json`).")
                    st.markdown(_tabla_clima_markdown(live))
        elif st.session_state.get(sk_res) is not None:
            st.caption("Última consulta en vivo en esta sesión:")
            st.markdown(_tabla_clima_markdown(st.session_state[sk_res]))


def render_vista_previa_ficheros_fase(
    st, fase: FaseKDD, base: Path, *, widget_scope: str = "kdd_principal"
) -> None:
    """Expanders anidados: un expander por fichero del repo referenciado en la fase."""
    if fase.orden in (1, 2):
        render_vista_previa_fase_ingesta_1_2(st, fase, base, widget_scope=widget_scope)
        return

    paths = _resolver_ficheros_fase(base, fase)
    if not paths:
        st.caption(
            "No hay rutas de fichero del repositorio resueltas en **Stack / script** "
            "(solo componentes como Kafka o Spark no muestran vista previa aquí)."
        )
        return

    st.markdown("**Ficheros del repositorio (5 primeras y 5 últimas líneas)**")
    for idx, p in enumerate(paths):
        rel = p.relative_to(base.resolve())
        head, tail, err = preview_primeras_ultimas_lineas(p)
        safe_key = str(rel).replace("/", "_").replace(" ", "_")
        with st.expander(
            f"`{rel}`",
            expanded=False,
            key=f"{widget_scope}_vf_gen_{fase.orden}_{idx}_{safe_key}",
        ):
            if err:
                st.warning(err)
                continue
            lang = _lang_por_sufijo(p)
            st.caption("Primeras 5 líneas")
            st.code(head or "", language=lang)
            if tail:
                st.caption("Últimas 5 líneas")
                st.code(tail, language=lang)
