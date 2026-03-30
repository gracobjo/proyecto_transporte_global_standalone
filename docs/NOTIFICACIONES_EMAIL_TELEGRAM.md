# Notificaciones por correo (SMTP) y Telegram

Funcionalidades opcionales para alertas operativas del cuadro de mando, scripts y eventos. La configuración vive en `.env` en la raíz del proyecto (cargada por `config.py` al importar).

---

## Correo electrónico (SMTP)

### Variables de entorno

| Variable | Descripción |
|----------|-------------|
| `SIMLOG_SMTP_HOST` | Servidor SMTP (ej. `smtp.gmail.com`). Si está vacío, el envío queda deshabilitado. |
| `SIMLOG_SMTP_PORT` | Puerto (por defecto `587`). |
| `SIMLOG_SMTP_TLS` | `1` (por defecto) para STARTTLS; `0` para conexión sin STARTTLS tras conectar. |
| `SIMLOG_SMTP_USER` | Usuario de autenticación (suele ser el correo). |
| `SIMLOG_SMTP_PASSWORD` | Contraseña o **contraseña de aplicación** (Gmail: 16 caracteres; se pueden pegar con o sin espacios; el código elimina espacios). |
| `SIMLOG_SMTP_FROM` | Remitente visible; si falta, se usa `SIMLOG_SMTP_USER`. |

Opcionales para destinatarios de avisos automáticos del pipeline:

| Variable | Descripción |
|----------|-------------|
| `SIMLOG_NOTIFY_EMAIL_TO` | Lista separada por comas o `;` de destinatarios. |
| `SIMLOG_NOTIFY_EMAIL_FALLBACK_SMTP_USER` | Si no quieres usar el remitente como destino por defecto, pon `0`. |
| `SIMLOG_NOTIFY_EMAIL_PIPELINE` | `0` desactiva correos automáticos tras ingesta/Spark (por defecto activo si SMTP está bien configurado). |

### Implementación

- Módulo: `servicios/notificaciones_correo.py`
- Funciones principales: `smtp_configurado()`, `enviar_correo_texto(destinatarios, asunto, cuerpo)`
- Errores frecuentes (Microsoft 365, Gmail, TLS) se traducen a mensajes en español en `_explicar_error_smtp`.

### Gmail

1. Activa verificación en dos pasos en la cuenta Google.
2. Crea una **contraseña de aplicación**: [App passwords](https://myaccount.google.com/apppasswords).
3. Usa `smtp.gmail.com`, puerto `587`, TLS activo.

---

## Telegram

### Variables de entorno

| Variable | Descripción |
|----------|-------------|
| `SIMLOG_TELEGRAM_BOT_TOKEN` | Token del bot (lo da @BotFather en Telegram). |
| `SIMLOG_TELEGRAM_CHAT_ID` | ID del chat o grupo que recibirá los mensajes (el usuario debe haber escrito `/start` al bot antes). |

| Variable | Descripción |
|----------|-------------|
| `SIMLOG_TELEGRAM_ENABLED` | `0` desactiva el envío aunque existan token y chat. |
| `SIMLOG_TELEGRAM_API_TIMEOUT_SEC` | Timeout de la llamada HTTP (por defecto 15 s). |

### Implementación

- Módulo: `servicios/notificaciones_telegram.py`
- Funciones: `telegram_configurado()`, `enviar_telegram_texto(texto)`, `notificar_cuadro_mando(titulo, ok, detalle)`
- Usa la API HTTP `https://api.telegram.org/bot<TOKEN>/sendMessage` vía `requests`.

### Integración con otros módulos

- `servicios/notificaciones_eventos.py` puede orquestar avisos según eventos del sistema.
- Tras cambiar `.env`, **reinicia** Streamlit y cualquier proceso que ya hubiera importado `config`.

---

## Comprobación manual

Desde la raíz del proyecto, con el venv activado y `.env` presente:

```bash
python3 - <<'PY'
from servicios.notificaciones_correo import enviar_correo_texto, smtp_configurado
from servicios.notificaciones_telegram import enviar_telegram_texto, telegram_configurado
from config import destinatarios_notificacion_email_pipeline

if smtp_configurado():
    dest = destinatarios_notificacion_email_pipeline()
    ok, msg = enviar_correo_texto(dest, "SIMLOG prueba", "Cuerpo de prueba.")
    print("Correo:", ok, msg)
if telegram_configurado():
    ok, msg = enviar_telegram_texto("SIMLOG prueba Telegram.")
    print("Telegram:", ok, msg)
PY
```

No subas `.env` al repositorio; mantén credenciales fuera de Git.
