"""
Notificaciones unificadas (Telegram + correo) al terminar ingesta, Spark o fases KDD desde Streamlit.

- Telegram: si `SIMLOG_TELEGRAM_*` está configurado (por defecto activo).
- Correo: si SMTP está configurado, `SIMLOG_NOTIFY_EMAIL_PIPELINE` no es 0 y hay destinatarios
  (`SIMLOG_NOTIFY_EMAIL_TO` o, por defecto, el mismo buzón que `SIMLOG_SMTP_USER`).
"""
from __future__ import annotations

from datetime import datetime, timezone

from config import (
    PROJECT_DISPLAY_NAME,
    destinatarios_notificacion_email_pipeline,
    notificaciones_email_pipeline_habilitadas,
)
from servicios.notificaciones_correo import enviar_correo_texto, smtp_configurado
from servicios.notificaciones_telegram import notificar_cuadro_mando


def notificar_ejecucion_pipeline(titulo: str, ok: bool, detalle: str = "") -> None:
    """
    Avisa por Telegram (si procede) y por correo (SMTP + destinatarios de pipeline).
    No lanza excepciones (no debe tumbar Streamlit).
    """
    try:
        notificar_cuadro_mando(titulo, ok, detalle)
    except Exception:
        pass
    if not notificaciones_email_pipeline_habilitadas():
        return
    dests = destinatarios_notificacion_email_pipeline()
    if not dests or not smtp_configurado():
        return
    icon = "OK" if ok else "ERROR"
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    body = (
        f"{PROJECT_DISPLAY_NAME}\n"
        f"Estado: {icon}\n"
        f"Evento: {titulo}\n"
        f"{ts}\n\n"
        f"{(detalle or '').strip()[:3500]}"
    )
    subj = f"[SIMLOG] {titulo} — {icon}"
    try:
        enviar_correo_texto(dests, subj, body)
    except Exception:
        pass
