#!/usr/bin/env python3
import logging
import os
import time
import requests
import paho.mqtt.publish as publish

# ==========================================================
# Configuration via variables d’environnement
# ==========================================================

OPENWEATHER_APP_ID = os.getenv("OPENWEATHER_APP_ID")
OPENWEATHER_CITY_ID = os.getenv("OPENWEATHER_CITY_ID")

MQTT_SERVICE_HOST = os.getenv("MQTT_SERVICE_HOST", "localhost")
MQTT_SERVICE_PORT = int(os.getenv("MQTT_SERVICE_PORT", 1883))
MQTT_SERVICE_TOPIC = os.getenv("MQTT_SERVICE_TOPIC", "openweather")
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

if MQTT_USERNAME is None or MQTT_PASSWORD is None:
    raise RuntimeError("MQTT_USERNAME and MQTT_PASSWORD must be set")

logger.info("Starting OpenWeather MQTT publisher")
logger.info(f"MQTT broker : {MQTT_SERVICE_HOST}:{MQTT_SERVICE_PORT}")
logger.info(f"MQTT topic  : {MQTT_SERVICE_TOPIC}")

# ==========================================================
# Helpers
# ==========================================================

def flatten_dict(data, parent_key="", sep="/"):
    items = {}
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.update(flatten_dict(v, new_key, sep))
    elif isinstance(data, list):
        for i, v in enumerate(data):
            new_key = f"{parent_key}{sep}{i}"
            items.update(flatten_dict(v, new_key, sep))
    else:
        items[parent_key] = data
    return items


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
        logger.error(f"OpenWeather HTTP {r.status_code}: {r.text}")
        return None

    data = r.json()

    if "dt" not in data:
        logger.error(f"Invalid OpenWeather payload: {data}")
        return None

    # Normalisation pluie
    data.setdefault("rain", {})
    data["rain"].setdefault("1h", 0)
    data["rain"].setdefault("3h", 0)

    return data


def publish_weather(data):
    flat = flatten_dict(data)
    msgs = []

    for k, v in sorted(flat.items()):
        topic = f"{MQTT_SERVICE_TOPIC}/{k}"
        msgs.append({"topic": topic, "payload": str(v), "retain": True})
        logger.info(f"{topic:45} -> {v}")

    publish.multiple(
        msgs,
        hostname=MQTT_SERVICE_HOST,
        port=MQTT_SERVICE_PORT,
        client_id=MQTT_CLIENT_ID,
        auth={
            "username": MQTT_USERNAME,
            "password": MQTT_PASSWORD,
        },
    )


# ==========================================================
# Main loop
# ==========================================================

last_dt = 0

while True:
    try:
        logger.info("Fetching weather data from OpenWeather")
        weather = fetch_weather()

        if weather and weather["dt"] > last_dt:
            last_dt = weather["dt"]
            publish_weather(weather)
        else:
            logger.info("No new weather data")

    except Exception:
        logger.exception("Unexpected error")

    time.sleep(UPDATE_INTERVAL)
