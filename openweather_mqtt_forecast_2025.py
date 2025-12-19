#!/usr/bin/env python3
import logging
import os
import time
import json
import requests
import paho.mqtt.publish as publish

# ==========================================================
# Configuration via variables d’environnement
# ==========================================================

OPENWEATHER_APP_ID = os.getenv("OPENWEATHER_APP_ID")
OPENWEATHER_CITY_ID = os.getenv("OPENWEATHER_CITY_ID")

MQTT_SERVICE_HOST = os.getenv("MQTT_SERVICE_HOST", "localhost")
MQTT_SERVICE_PORT = int(os.getenv("MQTT_SERVICE_PORT", 1883))

# IMPORTANT : topic final unique attendu par ESPHome
# Exemple: home/lcdmeteo
MQTT_SERVICE_TOPIC = os.getenv("MQTT_SERVICE_TOPIC", "home/lcdmeteo")

MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "openweather-mqtt-service")
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

UNITS = os.getenv("OPENWEATHER_UNITS", "metric")
LANG = os.getenv("OPENWEATHER_LANG", "fr")

UPDATE_INTERVAL = int(os.getenv("UPDATE_INTERVAL", 300))  # secondes

# ==========================================================
# Logging
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)8s %(message)s",
)
logger = logging.getLogger(MQTT_CLIENT_ID)

# ==========================================================
# Sanity checks
# ==========================================================

if not OPENWEATHER_APP_ID or not OPENWEATHER_CITY_ID:
    raise RuntimeError("OPENWEATHER_APP_ID and OPENWEATHER_CITY_ID must be set")

if not MQTT_USERNAME or not MQTT_PASSWORD:
    raise RuntimeError("MQTT_USERNAME and MQTT_PASSWORD must be set")

logger.info("Starting OpenWeather MQTT publisher (single JSON retained)")
logger.info(f"MQTT broker : {MQTT_SERVICE_HOST}:{MQTT_SERVICE_PORT}")
logger.info(f"MQTT topic  : {MQTT_SERVICE_TOPIC}")
logger.info(f"Update interval : {UPDATE_INTERVAL}s")

# ==========================================================
# OpenWeather fetchers
# ==========================================================

def fetch_weather():
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "id": OPENWEATHER_CITY_ID,
        "appid": OPENWEATHER_APP_ID,
        "units": UNITS,
        "lang": LANG,
    }

    r = requests.get(url, params=params, timeout=10)
    if r.status_code != 200:
        logger.error(f"Weather HTTP {r.status_code}: {r.text}")
        return None

    data = r.json()
    if "dt" not in data:
        logger.error(f"Invalid weather payload: {data}")
        return None

    # Normalisation pluie (si absent)
    data.setdefault("rain", {})
    data["rain"].setdefault("1h", 0)
    data["rain"].setdefault("3h", 0)

    return data


def fetch_forecast():
    url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {
        "id": OPENWEATHER_CITY_ID,
        "appid": OPENWEATHER_APP_ID,
        "units": UNITS,
        "lang": LANG,
    }

    r = requests.get(url, params=params, timeout=10)
    if r.status_code != 200:
        logger.error(f"Forecast HTTP {r.status_code}: {r.text}")
        return None

    data = r.json()
    if "list" not in data:
        logger.error(f"Invalid forecast payload: {data}")
        return None

    return data

# ==========================================================
# JSON builder (format attendu ESPHome)
# ==========================================================

def build_meteo_json(weather, forecast):
    """
    Produit un JSON unique sur home/lcdmeteo (retain=true), style:

    {
      "current": {...},
      "timezone": 3600,
      "forecast": {
        "0": { "temp": 14.3, "desc": "..." },
        "1": { "temp": 13.3, "desc": "..." }
      }
    }
    """

    payload = {
        "timezone": weather.get("timezone"),
        "current": {
            "sys": {
                "sunrise": weather["sys"]["sunrise"],
                "sunset": weather["sys"]["sunset"],
            },
            "main": {
                "temp": round(weather["main"]["temp"], 1),
            },
            "weather": [
                {"description": weather["weather"][0]["description"]}
            ],
        },
        "forecast": {}
    }

    # forecast optionnel
    if forecast and "list" in forecast and len(forecast["list"]) > 0:
        # indices typiques:
        # 0  = +3h
        # 8  = +24h (8 * 3h)
        mapping = {"0": 0, "1": 8}

        for key, idx in mapping.items():
            try:
                item = forecast["list"][idx]
                payload["forecast"][key] = {
                    "temp": round(item["main"]["temp"], 1),
                    "desc": item["weather"][0]["description"],
                }
            except (IndexError, KeyError, TypeError):
                logger.warning(f"Forecast slot {key} not available")

    return payload

# ==========================================================
# MQTT publish (single retained JSON)
# ==========================================================

def publish_json(payload):
    publish.single(
        topic=MQTT_SERVICE_TOPIC,               # ex: home/lcdmeteo
        payload=json.dumps(payload, ensure_ascii=False),
        hostname=MQTT_SERVICE_HOST,
        port=MQTT_SERVICE_PORT,
        client_id=MQTT_CLIENT_ID,
        retain=True,
        auth={
            "username": MQTT_USERNAME,
            "password": MQTT_PASSWORD,
        },
    )
    logger.info(f"Published retained JSON on {MQTT_SERVICE_TOPIC}")

# ==========================================================
# Main loop
# ==========================================================

while True:
    try:
        logger.info("Fetching current weather")
        weather = fetch_weather()

        logger.info("Fetching forecast")
        forecast = fetch_forecast()

        if weather:
            payload = build_meteo_json(weather, forecast)
            publish_json(payload)
        else:
            logger.warning("No weather data received (skip publish)")

    except Exception:
        logger.exception("Unexpected error")

    time.sleep(UPDATE_INTERVAL)
