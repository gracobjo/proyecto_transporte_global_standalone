"""
Envío de correo (SMTP) para alertas operativas del cuadro de mando.
Configuración vía variables de entorno `SIMLOG_SMTP_*` (ver `config.py`).
"""
from __future__ import annotations

import smtplib
from email.mime.text import MIMEText
from typing import List, Tuple

from config import SMTP_FROM, SMTP_HOST, SMTP_PASSWORD, SMTP_PORT, SMTP_USE_TLS, SMTP_USER


def smtp_configurado() -> bool:
    return bool((SMTP_HOST or "").strip())


def enviar_correo_texto(
    destinatarios: List[str],
    asunto: str,
    cuerpo: str,
) -> Tuple[bool, str]:
    """
    Envía un correo en texto plano. `destinatarios` sin vacíos.
    Devuelve (ok, mensaje).
    """
    tos = [d.strip() for d in destinatarios if d and str(d).strip()]
    if not tos:
        return False, "No hay direcciones de correo válidas."
    if not smtp_configurado():
        return False, (
            "SMTP no configurado. Define SIMLOG_SMTP_HOST (y opcionalmente "
            "SIMLOG_SMTP_USER, SIMLOG_SMTP_PASSWORD, SIMLOG_SMTP_FROM)."
        )
    from_addr = (SMTP_FROM or SMTP_USER or "").strip()
    if not from_addr:
        return False, "Falta remitente: SIMLOG_SMTP_FROM o SIMLOG_SMTP_USER."

    msg = MIMEText(cuerpo, "plain", "utf-8")
    msg["Subject"] = asunto
    msg["From"] = from_addr
    msg["To"] = ", ".join(tos)

    try:
        if SMTP_USE_TLS:
            server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
            server.ehlo()
            server.starttls()
            server.ehlo()
        else:
            server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30)
        if SMTP_USER and SMTP_PASSWORD:
            server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(from_addr, tos, msg.as_string())
        server.quit()
        return True, f"Enviado a {len(tos)} destinatario(s)."
    except Exception as e:
        return False, str(e)[:500]
