# utils/discord_notify.py
import requests
import os

def enviar_resumen_discord(mensaje):
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        print("⚠️ DISCORD_WEBHOOK_URL no está configurado.")
        return

    data = {"content": mensaje}
    try:
        response = requests.post(webhook_url, json=data)
        if response.status_code != 204:
            print(f"Error al notificar a Discord: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error al enviar mensaje a Discord: {e}")
