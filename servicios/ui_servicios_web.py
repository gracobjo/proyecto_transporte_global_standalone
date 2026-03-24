"""
URLs y credenciales de las interfaces web de cada servicio (configurables por entorno).

Variables recomendadas (ejemplo en `.env` o shell):
  SIMLOG_UI_HDFS_URL, SIMLOG_UI_HDFS_PORT, SIMLOG_UI_HDFS_USER, SIMLOG_UI_HDFS_PASSWORD
  … análogo para KAFKA, CASSANDRA, SPARK, HIVE, AIRFLOW, NIFI

Si SIMLOG_UI_REVEAL_SECRETS=1, se muestran contraseñas en claro (solo entornos de confianza).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

# Host base para URLs por defecto (cambiar si accedes desde otra máquina)
_UI_HOST = os.environ.get("SIMLOG_UI_HOST", "127.0.0.1")


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def _reveal_secrets() -> bool:
    return _env("SIMLOG_UI_REVEAL_SECRETS", "").lower() in ("1", "true", "yes", "si")


@dataclass(frozen=True)
class InterfazWebServicio:
    """Metadatos para enlazar la UI de un servicio desde el dashboard."""

    servicio_id: str
    nombre: str
    url: str
    puerto: str
    usuario: str
    password_env: str  # nombre de variable de entorno (no el valor)
    nota: str

    def password_valor(self) -> str:
        return _env(self.password_env, "")

    def usuario_mostrar(self) -> str:
        u = _env(f"SIMLOG_UI_{self.servicio_id.upper()}_USER", "")
        if u:
            return u
        return self.usuario if self.usuario else "—"

    def password_mostrar(self) -> str:
        v = self.password_valor()
        if not v:
            return "— (configura " + self.password_env + ")"
        if _reveal_secrets():
            return v
        return "•••••••• (definida en " + self.password_env + ")"


def _defaults() -> dict[str, InterfazWebServicio]:
    """URLs por defecto (literales); en tiempo de uso aplicar `url_efectiva()` que prioriza env."""
    h = _UI_HOST
    return {
        "hdfs": InterfazWebServicio(
            servicio_id="hdfs",
            nombre="HDFS NameNode",
            url=f"http://{h}:9870",
            puerto="9870",
            usuario="",
            password_env="SIMLOG_UI_HDFS_PASSWORD",
            nota="Interfaz web del NameNode (overview, DataNodes, explorador). Sin auth por defecto.",
        ),
        "kafka": InterfazWebServicio(
            servicio_id="kafka",
            nombre="Kafka (UI opcional)",
            url=f"http://{h}:8095",
            puerto="8095",
            usuario="",
            password_env="SIMLOG_UI_KAFKA_PASSWORD",
            nota="Kafka no trae UI oficial. Ajusta URL/puerto si usas Kafdrop, AKHQ, Kafka UI, etc.",
        ),
        "cassandra": InterfazWebServicio(
            servicio_id="cassandra",
            nombre="Cassandra",
            url="",
            puerto="9042",
            usuario="cassandra",
            password_env="SIMLOG_UI_CASSANDRA_PASSWORD",
            nota="Sin UI HTTP nativa. 9042 = CQL (cqlsh). Si instalas Reaper/Studio, define SIMLOG_UI_CASSANDRA_URL.",
        ),
        "spark": InterfazWebServicio(
            servicio_id="spark",
            nombre="Spark Master / History",
            url=f"http://{h}:8081",
            puerto="8081",
            usuario="",
            password_env="SIMLOG_UI_SPARK_PASSWORD",
            nota="Master UI (standalone). 8081 evita choque con Airflow en 8080. Ajusta si tu cluster usa otro puerto.",
        ),
        "hive": InterfazWebServicio(
            servicio_id="hive",
            nombre="Hive / Hue (opcional)",
            url=f"http://{h}:8888",
            puerto="8888",
            usuario="",
            password_env="SIMLOG_UI_HIVE_PASSWORD",
            nota="HiveServer2 JDBC suele ser puerto 10000. La URL web suele ser Hue u otra consola si la instalas.",
        ),
        "airflow": InterfazWebServicio(
            servicio_id="airflow",
            nombre="Airflow",
            url=f"http://{h}:8080",
            puerto="8080",
            usuario="admin",
            password_env="SIMLOG_UI_AIRFLOW_PASSWORD",
            nota="Web / API Airflow 2.x. Crea usuario con `airflow users create` y define credenciales en variables de entorno.",
        ),
        "nifi": InterfazWebServicio(
            servicio_id="nifi",
            nombre="Apache NiFi",
            url=f"https://{h}:8443/nifi",
            puerto="8443",
            usuario="",
            password_env="SIMLOG_UI_NIFI_PASSWORD",
            nota="HTTPS. Credenciales iniciales en logs de NiFi o configuración de seguridad.",
        ),
    }


_CACHE: Optional[dict[str, InterfazWebServicio]] = None


def obtener_interfaz_web(servicio_id: str) -> InterfazWebServicio:
    global _CACHE
    if _CACHE is None:
        _CACHE = _defaults()
    return _CACHE[servicio_id]


def listar_interfaces_web() -> list[InterfazWebServicio]:
    from servicios.gestion_servicios import ORDEN_SERVICIOS

    global _CACHE
    if _CACHE is None:
        _CACHE = _defaults()
    return [_CACHE[sid] for sid in ORDEN_SERVICIOS if sid in _CACHE]


def render_bloque_interfaz_web(sid: str) -> None:
    """Streamlit: expander con URL, puerto, usuario/contraseña y botón de enlace."""
    import streamlit as st

    ui = obtener_interfaz_web(sid)
    url = url_efectiva(ui)
    pe = puerto_efectivo(ui)
    with st.expander("Interfaz web · URL y credenciales", expanded=False):
        st.caption(ui.nota)
        st.markdown(f"**Puerto (referencia):** `{pe}`")
        st.markdown(f"**Usuario:** `{ui.usuario_mostrar()}`")
        st.markdown(f"**Contraseña:** `{ui.password_mostrar()}`")
        st.caption(
            "Configura `SIMLOG_UI_<SERVICIO>_URL`, `_PORT`, `_USER`, `_PASSWORD`. "
            "Para mostrar contraseña en claro: `SIMLOG_UI_REVEAL_SECRETS=1` (solo entornos de confianza)."
        )
        if url:
            try:
                st.link_button("Abrir interfaz en nueva pestaña", url=url, width="stretch", key=f"link_ui_{sid}")
            except Exception:
                st.markdown(
                    f'<a href="{url}" target="_blank" rel="noopener noreferrer">Abrir interfaz en nueva pestaña</a>',
                    unsafe_allow_html=True,
                )
        else:
            st.info("Sin URL HTTP por defecto. Define `SIMLOG_UI_%s_URL` si tienes consola web." % sid.upper())


def render_sidebar_enlaces_ui() -> None:
    """Enlaces compactos en la barra lateral."""
    import streamlit as st

    with st.expander("Interfaces web del stack", expanded=False):
        st.caption("Abre cada UI en una pestaña nueva. Detalle de credenciales en **Servicios**.")
        for ui in listar_interfaces_web():
            url = url_efectiva(ui)
            if not url:
                st.caption(f"**{ui.nombre}:** sin URL (configura SIMLOG_UI_{ui.servicio_id.upper()}_URL)")
                continue
            try:
                st.link_button(
                    f"Abrir {ui.nombre}",
                    url=url,
                    width="stretch",
                    key=f"sb_ui_{ui.servicio_id}",
                )
            except Exception:
                st.markdown(f"- [{ui.nombre}]({url})")


def url_efectiva(ui: InterfazWebServicio) -> str:
    """URL final: variable `SIMLOG_UI_<ID>_URL` tiene prioridad sobre el valor por defecto."""
    custom = _env(f"SIMLOG_UI_{ui.servicio_id.upper()}_URL", "")
    return custom if custom else ui.url


def puerto_efectivo(ui: InterfazWebServicio) -> str:
    p = _env(f"SIMLOG_UI_{ui.servicio_id.upper()}_PORT", "")
    return p if p else ui.puerto
