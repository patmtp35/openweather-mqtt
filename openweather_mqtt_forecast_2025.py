#!/usr/bin/env python3
import logging
import os
import time
import json
import requests
import paho.mqtt.publish as publish

# ==========================================================
# Configuration via variables d’environnement (.env)
# ==========================================================

OPENWEATHER_APP_ID = os.getenv("OPENWEATHER_APP_ID")
OPENWEATHER_CITY_ID = os.getenv("OPENWEATHER_CITY_ID")

MQTT_SERVICE_HOST = os.getenv("MQTT_SERVICE_HOST", "localhost")
MQTT_SERVICE_PORT = int(os.getenv("MQTT_SERVICE_PORT", 1883))
MQTT_SERVICE_TOPIC = os.getenv("MQTT_SERVICE_TOPIC", "home/lcdmeteo")

MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "openweather-mqtt-service")
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

UNITS = os.getenv("OPENWEATHER_UNITS", "metric")
LANG = os.getenv("OPENWEATHER_LANG", "fr")
UPDATE_INTERVAL = int(os.getenv("UPDATE_INTERVAL", 300))

# ==========================================================
# Logging
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)8s %(message)s",
)
logger = logging.getLogger(MQTT_CLIENT_ID)

# ==========================================================
# Sanity checks (FAIL FAST)
# ==========================================================

if not OPENWEATHER_APP_ID or not OPENWEATHER_CITY_ID:
    raise RuntimeError("OPENWEATHER_APP_ID and OPENWEATHER_CITY_ID must be set")

if not MQTT_USERNAME or not MQTT_PASSWORD:
    raise RuntimeError("MQTT_USERNAME and MQTT_PASSWORD must be set")

logger.info("Starting OpenWeather MQTT Forecast Publisher")
logger.info(f"MQTT broker : {MQTT_SERVICE_HOST}:{MQTT_SERVICE_PORT}")
logger.info(f"MQTT topic  : {MQTT_SERVICE_TOPIC}")
logger.info(f"Update interval : {UPDATE_INTERVAL}s")

# ==========================================================
# OpenWeather fetchers (HTTPS obligatoire)
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

    # Normalisation pluie / neige
    data.setdefault("rain", {})
    data["rain"].setdefault("1h", 0)
    data["rain"].setdefault("3h", 0)

    data.setdefault("snow", {})
    data["snow"].setdefault("1h", 0)
    data["snow"].setdefault("3h", 0)

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
# JSON builder (STRUCTURE ORIGINALE + ENRICHIE)
# ==========================================================

def build_meteo_json(weather, forecast):
    payload = {
        # === STRUCTURE HISTORIQUE (NE PAS MODIFIER) ===
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

    # === FORECAST HISTORIQUE (déjà modifié hier) ===
    if forecast and "list" in forecast and len(forecast["list"]) > 0:
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

    # === DONNÉES SUPPLÉMENTAIRES (SAFE / NON CASSANTES) ===
    payload["current"]["main"].update({
        "humidity": weather["main"].get("humidity"),
        "pressure": weather["main"].get("pressure"),
        "feels_like": weather["main"].get("feels_like"),
        "temp_min": weather["main"].get("temp_min"),
        "temp_max": weather["main"].get("temp_max"),
    })

    payload["current"]["wind"] = weather.get("wind", {})
    payload["current"]["clouds"] = weather.get("clouds", {})
    payload["current"]["visibility"] = weather.get("visibility")

    payload["current"]["rain"] = {
        "1h": weather["rain"].get("1h", 0),
        "3h": weather["rain"].get("3h", 0),
    }

    payload["current"]["snow"] = {
        "1h": weather["snow"].get("1h", 0),
        "3h": weather["snow"].get("3h", 0),
    }

    payload["current"]["weather_full"] = weather.get("weather", [])

    payload["meta"] = {
        "dt": weather.get("dt"),
        "updated_at": int(time.time()),
        "city": weather.get("name"),
    }

    return payload

# ==========================================================
# MQTT publish (JSON unique retained)
# ==========================================================

def publish_json(payload):
    publish.single(
        topic=MQTT_SERVICE_TOPIC,
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
    logger.info("Published retained JSON")

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
