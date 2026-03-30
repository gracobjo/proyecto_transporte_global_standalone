#!/usr/bin/env python3
"""
Programación de envíos a Telegram (pruebas / demos).

Configura en `.env` o en el entorno:
  SIMLOG_TELEGRAM_BOT_TOKEN
  SIMLOG_TELEGRAM_CHAT_ID

No guardes el token en este archivo; usa variables de entorno.
"""
import os
import sys
import time

# Asegura importación del proyecto
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import schedule

from servicios.notificaciones_telegram import enviar_telegram_texto, telegram_configurado


def send_telegram_message(message: str) -> None:
    """Compatibilidad con scripts antiguos: envía un mensaje de texto."""
    ok, err = enviar_telegram_texto(message)
    if ok:
        print("Mensaje enviado a Telegram.")
    else:
        print("Error:", err)


def programar_envio(mensaje: str, hora: str) -> None:
    """Programa un mensaje diario a la hora indicada (formato 24h, ej. 19:25)."""

    def tarea() -> None:
        send_telegram_message(mensaje)

    schedule.every().day.at(hora).do(tarea)
    print(f"Mensaje programado: {mensaje!r} a las {hora}")


if __name__ == "__main__":
    if not telegram_configurado():
        print(
            "Telegram no configurado. Define SIMLOG_TELEGRAM_BOT_TOKEN y "
            "SIMLOG_TELEGRAM_CHAT_ID en .env o en el entorno.",
            file=sys.stderr,
        )
        sys.exit(1)

    mensaje = os.environ.get("SIMLOG_TELEGRAM_TEST_MSG", "Mensaje de prueba (mensajeAutomatico.py)")
    hora = os.environ.get("SIMLOG_TELEGRAM_SCHEDULE_AT", "19:25")
    programar_envio(mensaje, hora)
    print("Esperando tareas programadas… (Ctrl+C para salir)")
    while True:
        schedule.run_pending()
        time.sleep(1)
