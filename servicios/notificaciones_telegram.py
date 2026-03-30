"""
Notificaciones a Telegram para el cuadro de mando y scripts auxiliares.

Configuración (`.env` o entorno del proceso):
  SIMLOG_TELEGRAM_BOT_TOKEN — token del bot (@BotFather)
  SIMLOG_TELEGRAM_CHAT_ID   — chat o grupo destino

Opcional: SIMLOG_TELEGRAM_ENABLED=0 desactiva el envío aunque haya credenciales.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Tuple

from config import PROJECT_DISPLAY_NAME, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)


def telegram_configurado() -> bool:
    if os.environ.get("SIMLOG_TELEGRAM_ENABLED", "1").strip() == "0":
        return False
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def enviar_telegram_texto(texto: str) -> Tuple[bool, str]:
    """
    Envía un mensaje de texto. Devuelve (ok, mensaje_error_o_ok).
    """
    if not telegram_configurado():
        return False, "Telegram no configurado (SIMLOG_TELEGRAM_BOT_TOKEN + SIMLOG_TELEGRAM_CHAT_ID)."
    if len(texto) > 4000:
        texto = texto[:3997] + "…"
    try:
        import requests

        timeout = float(os.environ.get("SIMLOG_TELEGRAM_API_TIMEOUT_SEC", "15"))
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        r = requests.post(
            url,
            data={"chat_id": TELEGRAM_CHAT_ID, "text": texto},
            timeout=timeout,
        )
        if r.status_code == 200:
            try:
                body = r.json()
                if body.get("ok"):
                    return True, "Enviado."
                desc = (body.get("description") or body) if isinstance(body, dict) else str(body)
                return False, f"Telegram API: {desc}"[:500]
            except Exception:
                return True, "Enviado."
        try:
            err_j = r.json()
            desc = err_j.get("description") or r.text
        except Exception:
            desc = r.text or str(r.status_code)
        return False, f"HTTP {r.status_code}: {desc}"[:500]
    except Exception as e:
        return False, str(e)[:500]


def notificar_cuadro_mando(titulo: str, ok: bool, detalle: str = "") -> None:
    """
    Notifica el resultado de una acción del cuadro de mando. No lanza excepciones
    (no debe tumbar Streamlit si falla la API). Los fallos se registran en el log.
    """
    try:
        if not telegram_configurado():
            return
        icon = "✅" if ok else "❌"
        lineas = [f"{icon} {PROJECT_DISPLAY_NAME} — Cuadro de mando", titulo]
        if detalle.strip():
            lineas.append(detalle.strip()[:800])
        ok_send, err = enviar_telegram_texto("\n".join(lineas))
        if not ok_send:
            logger.warning("Telegram sendMessage falló: %s", err)
    except Exception as ex:
        logger.warning("Telegram notificar_cuadro_mando: %s", ex)


def probar_telegram_desde_ui() -> Tuple[bool, str]:
    """
    Comprueba el token (getMe) y envía un mensaje de prueba al chat configurado.
    Devuelve (éxito, mensaje para mostrar en la UI).
    """
    if not telegram_configurado():
        return False, (
            "Falta `SIMLOG_TELEGRAM_BOT_TOKEN` o `SIMLOG_TELEGRAM_CHAT_ID` en `.env`. "
            "Reinicia Streamlit tras editar el archivo."
        )
    try:
        import requests

        timeout = float(os.environ.get("SIMLOG_TELEGRAM_API_TIMEOUT_SEC", "15"))
        me_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getMe"
        r = requests.get(me_url, timeout=timeout)
        if r.status_code != 200:
            return False, f"getMe HTTP {r.status_code}: {(r.text or '')[:300]}"
        me = r.json()
        if not me.get("ok"):
            return False, f"Token inválido o revocado: {json.dumps(me, ensure_ascii=False)[:400]}"

        texto = (
            f"SIMLOG — prueba desde cuadro de mando\n"
            f"Bot: @{me.get('result', {}).get('username', '?')}"
        )
        ok, msg = enviar_telegram_texto(texto)
        if ok:
            return True, "Mensaje de prueba enviado. Revisa Telegram (y spam del bot si aplica)."
        return False, (
            f"El token es válido pero no se pudo enviar al chat_id={TELEGRAM_CHAT_ID}. "
            f"¿Has escrito /start al bot? Detalle: {msg}"
        )
    except Exception as e:
        return False, str(e)[:500]
