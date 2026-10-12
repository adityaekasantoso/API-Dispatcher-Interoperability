from flask import Flask, jsonify
import os
import configparser
from datetime import datetime, timedelta
import mysql.connector
import pyodbc
import sys

BASE_DIR = (
    os.path.dirname(sys.executable)
    if getattr(sys, "frozen", False)
    else os.path.dirname(os.path.abspath(__file__))
)

CONFIG_PATH = os.path.join(BASE_DIR, "setup.ini")

config = configparser.ConfigParser()
config.read(CONFIG_PATH, encoding="utf-8")

app = Flask(__name__)

mysql_config = {
    "host": config.get("mysql", "host", fallback="127.0.0.1"),
    "port": config.getint("mysql", "port", fallback=3306),
    "user": config.get("mysql", "user", fallback="root"),
    "password": config.get("mysql", "password", fallback=""),
    "database": config.get("mysql", "database", fallback="test"),
    "autocommit": True,
}

sqlserver_config = (
    f"DRIVER={{{config.get('sqlserver', 'driver', fallback='ODBC Driver 17 for SQL Server')}}};"
    f"SERVER={config.get('sqlserver', 'server', fallback='127.0.0.1')},{config.get('sqlserver', 'port', fallback='1433')};"
    f"DATABASE={config.get('sqlserver', 'database', fallback='master')};"
    f"UID={config.get('sqlserver', 'user', fallback='sa')};"
    f"PWD={config.get('sqlserver', 'password', fallback='')};"
    f"TrustServerCertificate=yes;"
)


def serialize_value(value):
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.isoformat()

    if isinstance(value, timedelta):
        return str(value)

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")

    return value


@app.route("/positions/excera")
def excera():
    conn = None

    try:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT radio_id, updated, lat, lon, speed
            FROM (
                SELECT
                    radio_id,
                    updated,
                    lat,
                    lon,
                    speed,
                    ROW_NUMBER() OVER (
                        PARTITION BY radio_id
                        ORDER BY updated DESC
                    ) AS rn
                FROM tmp_pos
                WHERE updated >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
            ) t
            WHERE t.rn = 1
        """)

        rows = cursor.fetchall()

        records = []

        for idx, row in enumerate(rows, start=1):
            records.append({
                "id": idx,
                "address": serialize_value(row.get("radio_id")),
                "latitude": float(row["lat"]) if row["lat"] else 0,
                "longitude": float(row["lon"]) if row["lon"] else 0,
                "speed": int(row["speed"]) if row["speed"] else 0,
                "timestamp": serialize_value(row.get("updated"))
            })

        return jsonify({
            "records": records
        })

    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500

    finally:
        if conn:
            conn.close()


@app.route("/positions/walkifleet")
def walkifleet():
    conn = None

    try:
        conn = pyodbc.connect(sqlserver_config)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT userid, name, dt, latitude, longitude, speed
            FROM (
                SELECT
                    l.userid,
                    u.name,
                    l.dt,
                    l.latitude,
                    l.longitude,
                    l.speed,
                    ROW_NUMBER() OVER (
                        PARTITION BY l.userid
                        ORDER BY l.dt DESC
                    ) AS rn
                FROM locations l
                LEFT JOIN users u ON l.userid = u.id
                WHERE l.dt >= DATEADD(HOUR, -1, GETDATE())
            ) t
            WHERE t.rn = 1
        """)

        rows = cursor.fetchall()

        records = []

        for idx, row in enumerate(rows, start=1):
            records.append({
                "id": idx,
                "address": (
                    serialize_value(row.name)
                    if row.name
                    else str(row.userid).split("-")[-1].lstrip("0")
                ),
                "latitude": float(row.latitude) if row.latitude else 0,
                "longitude": float(row.longitude) if row.longitude else 0,
                "speed": int(row.speed) if row.speed else 0,
                "timestamp": serialize_value(row.dt)
            })

        return jsonify({
            "records": records
        })

    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True
    )