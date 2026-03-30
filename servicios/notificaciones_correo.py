"""
Envío de correo (SMTP) para alertas operativas del cuadro de mando.
Configuración vía variables de entorno `SIMLOG_SMTP_*` (ver `config.py`).
"""
from __future__ import annotations

import smtplib
from email.mime.text import MIMEText
from typing import List, Tuple

from config import SMTP_FROM, SMTP_HOST, SMTP_PASSWORD, SMTP_PORT, SMTP_USE_TLS, SMTP_USER


def _explicar_error_smtp(exc: BaseException) -> str:
    """
    Traduce errores frecuentes (p. ej. Microsoft 365 sin SMTP AUTH) a texto útil en español.
    """
    raw = str(exc)
    low = raw.lower()
    if "535" in raw and (
        "basic authentication" in low
        or "5.7.139" in raw
        or "authentication unsuccessful" in low
    ):
        return (
            "Microsoft 365 / Outlook rechazó el inicio de sesión SMTP (código 535): "
            "la autenticación básica (usuario + contraseña) está deshabilitada para este buzón o para el inquilino.\n\n"
            "Opciones:\n"
            "• Que un administrador de Microsoft 365 active «SMTP autenticado» / Authenticated SMTP "
            "para tu cuenta (Centro de administración de Exchange → buzones → correo electrónico → "
            "«Autenticación SMTP» o, en conjunto, directivas de autenticación).\n"
            "• Usar otro proveedor que permita SMTP con contraseña de aplicación, "
            "p. ej. Gmail: `smtp.gmail.com`, puerto 587, TLS, y una «contraseña de aplicación» "
            "(no la contraseña normal de la cuenta).\n"
            "• Para solo avisos automáticos, puedes usar Telegram (SIMLOG_TELEGRAM_*), sin SMTP.\n\n"
            f"Detalle técnico: {raw[:400]}"
        )
    if "534" in raw or ("5.7.0" in raw and "authentication" in low):
        return (
            "El servidor SMTP rechazó la autenticación. Revisa usuario, contraseña y que el "
            "proveedor permita el método de acceso (TLS en 587, o SSL en 465).\n\n"
            f"Detalle: {raw[:400]}"
        )
    return raw[:500]


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
        return False, _explicar_error_smtp(e)
