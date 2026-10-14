import socket
import sqlite3
import requests
import threading
import time
import configparser
import urllib3
import os
import sys
import json

from requests.auth import HTTPBasicAuth
from flask import Flask
from pyais import decode
from datetime import datetime, timezone


# ============================================================
# PATH / CONFIG
# ============================================================

BASE_DIR = (
    os.path.dirname(sys.executable)
    if getattr(sys, "frozen", False)
    else os.path.dirname(os.path.abspath(__file__))
)

CONFIG_PATH = os.path.join(BASE_DIR, "setup.ini")


config = configparser.ConfigParser()
config.read(CONFIG_PATH)


DB_NAME = config.get(
    "database",
    "db",
    fallback=os.path.join(BASE_DIR, "data.db")
)


# ============================================================
# CONSTANTS
# ============================================================

BUFFER_SIZE = 4096

FRAGMENT_TIMEOUT = 30
IDLE_DATA_TIMEOUT = 60
SOCKET_RECV_TIMEOUT = 10

urllib3.disable_warnings(
    urllib3.exceptions.InsecureRequestWarning
)


# ============================================================
# FLASK
# ============================================================

app = Flask(__name__)


# ============================================================
# LOCKS
# ============================================================

db_lock = threading.RLock()


# ============================================================
# WORKERS
# ============================================================

adsb_workers = {}
radio_workers = {}
ais_workers = {}


# ============================================================
# AIS MESSAGE TYPES
# ============================================================

VALID_MSG_TYPES = {
    1,   # Class A position
    2,   # Class A position
    3,   # Class A position
    4,   # Base station
    5,   # Static / Voyage
    18,  # Class B position
    19,  # Class B extended position
    24,  # Class B static data
}


POSITION_MSG_TYPES = {
    1,
    2,
    3,
    4,
    18,
    19,
}


STATIC_MSG_TYPES = {
    5,
    24,
}


# ============================================================
# NAV STATUS
# ============================================================

NAV_STATUS = {
    0: "Under way using engine",
    1: "At anchor",
    2: "Not under command",
    3: "Restricted manoeuverability",
    4: "Constrained by her draught",
    5: "Moored",
    6: "Aground",
    7: "Engaged in fishing",
    8: "Under way sailing",
    9: "Reserved for future amendment",
    10: "Reserved for future amendment",
    11: "Reserved for future amendment",
    12: "Reserved for future amendment",
    13: "Reserved for future amendment",
    14: "AIS-SART / MOB / EPIRB",
    15: "Not defined",
}


# ============================================================
# SHIP TYPE
# ============================================================

SHIP_TYPE = {
    0: "Not available",

    20: "Wing in ground (WIG)",
    21: "Wing in ground - reserved",
    22: "Wing in ground - reserved",
    23: "Wing in ground - reserved",
    24: "Wing in ground - reserved",
    25: "Wing in ground - reserved",
    26: "Wing in ground - reserved",
    27: "Wing in ground - reserved",
    28: "Wing in ground - reserved",
    29: "Wing in ground - reserved",

    30: "Fishing",
    31: "Towing",
    32: "Towing (large)",
    33: "Dredging / underwater ops",
    34: "Diving ops",
    35: "Military ops",
    36: "Sailing",
    37: "Pleasure craft",
    38: "Reserved",
    39: "Reserved",

    40: "High speed craft (HSC)",
    41: "HSC - Passenger",
    42: "HSC - Cargo",
    43: "HSC - Tender",
    44: "HSC - Pleasure craft",
    45: "HSC - Reserved",
    46: "HSC - Reserved",
    47: "HSC - Reserved",
    48: "HSC - Reserved",
    49: "HSC - No additional information",

    50: "Pilot vessel",
    51: "Search and rescue (SAR)",
    52: "Tug",
    53: "Port tender",
    54: "Anti-pollution equipment",
    55: "Law enforcement",
    56: "Spare - local vessel",
    57: "Spare - local vessel",
    58: "Medical transport",
    59: "Noncombatant ship",

    60: "Passenger",
    61: "Passenger - Hazardous category A",
    62: "Passenger - Hazardous category B",
    63: "Passenger - Hazardous category C",
    64: "Passenger - Hazardous category D",
    65: "Passenger - Other",
    66: "Reserved",
    67: "Reserved",
    68: "Reserved",
    69: "Passenger - No additional information",

    70: "Cargo",
    71: "Cargo - Hazardous category A",
    72: "Cargo - Hazardous category B",
    73: "Cargo - Hazardous category C",
    74: "Cargo - Hazardous category D",
    75: "Cargo - Other",
    76: "Reserved",
    77: "Reserved",
    78: "Reserved",
    79: "Cargo - No additional information",

    80: "Tanker",
    81: "Tanker - Hazardous category A",
    82: "Tanker - Hazardous category B",
    83: "Tanker - Hazardous category C",
    84: "Tanker - Hazardous category D",
    85: "Tanker - Other",
    86: "Reserved",
    87: "Reserved",
    88: "Reserved",
    89: "Tanker - No additional information",

    90: "Other",
    91: "Other - Hazardous category A",
    92: "Other - Hazardous category B",
    93: "Other - Hazardous category C",
    94: "Other - Hazardous category D",
    95: "Other - Reserved",
    96: "Other - Reserved",
    97: "Other - Reserved",
    98: "Other - Reserved",
    99: "Other - No additional information",
}


# ============================================================
# COUNTRY / MID
# ============================================================

COUNTRY = {
    # Europe
    "201": "Albania",
    "202": "Andorra",
    "203": "Austria",
    "204": "Portugal (Azores)",
    "205": "Belgium",
    "206": "Belarus",
    "207": "Bulgaria",
    "208": "Vatican City",
    "209": "Cyprus",
    "210": "Cyprus",
    "211": "Germany",
    "212": "Cyprus",
    "213": "Georgia",
    "214": "Moldova",
    "215": "Malta",
    "216": "Armenia",
    "218": "Germany",
    "219": "Denmark",
    "220": "Denmark",
    "224": "Spain",
    "226": "France",
    "230": "Finland",
    "231": "Faroe Islands",
    "232": "United Kingdom",
    "233": "United Kingdom",
    "234": "United Kingdom",
    "235": "United Kingdom",
    "236": "Gibraltar",
    "237": "Greece",
    "238": "Croatia",
    "239": "Greece",
    "240": "Albania",
    "241": "Netherlands",
    "242": "Portugal",
    "244": "Netherlands",
    "246": "Belarus",
    "248": "Croatia",
    "250": "Ireland",
    "251": "Iceland",
    "255": "Malta",
    "257": "Norway",
    "259": "Norway",
    "261": "Poland",
    "263": "Portugal",
    "266": "Romania",
    "268": "Hungary",
    "270": "Italy",
    "271": "Slovenia",
    "272": "San Marino",
    "273": "Ukraine",
    "274": "Czech Republic",
    "276": "Slovakia",
    "278": "Slovenia",
    "280": "Switzerland",
    "282": "Serbia",
    "283": "Montenegro",

    # North America / Caribbean
    "300": "Virgin Islands",
    "301": "Anguilla",
    "302": "Antigua and Barbuda",
    "303": "Bermuda",
    "304": "Canada",
    "305": "Canada",
    "306": "Netherlands (Caribbean)",
    "307": "Aruba",
    "308": "Dominica",
    "309": "Dominican Republic",
    "310": "United States",
    "311": "United States",
    "312": "United States",
    "316": "Canada",
    "338": "United States",
    "366": "United States",
    "367": "United States",
    "368": "United States",
    "369": "United States",

    # Central / South America
    "350": "Chile",
    "351": "Portugal",
    "352": "Luxembourg",
    "355": "Portugal (Madeira)",
    "360": "Egypt",
    "362": "Morocco",
    "370": "Panama",
    "371": "Panama",
    "372": "Panama",
    "373": "Panama",
    "374": "Nicaragua",
    "375": "Panama",
    "380": "Colombia",
    "385": "Barbados",
    "387": "Barbados",
    "400": "United Arab Emirates",

    # Asia
    "401": "Israel",
    "402": "Lebanon",
    "403": "Saudi Arabia",
    "404": "Kuwait",
    "405": "Pakistan",
    "406": "Afghanistan",
    "407": "Sri Lanka",
    "408": "Bahrain",
    "409": "Qatar",
    "410": "Jordan",
    "411": "Yemen",
    "412": "China",
    "413": "China",
    "414": "China",
    "415": "Mongolia",
    "416": "Taiwan",
    "417": "Sri Lanka",
    "418": "Nepal",
    "419": "India",
    "420": "Turkey",
    "430": "Japan",
    "431": "Japan",
    "432": "Japan",
    "440": "South Korea",
    "441": "South Korea",

    # Southeast Asia / Oceania
    "450": "Bangladesh",
    "455": "Maldives",
    "457": "Myanmar",
    "462": "Philippines",
    "463": "Philippines",
    "470": "Turkey",
    "502": "Australia",
    "503": "Australia",
    "504": "New Zealand",
    "505": "Papua New Guinea",
    "506": "Fiji",
    "507": "Solomon Islands",
    "508": "Vanuatu",
    "515": "Cambodia",
    "520": "Marshall Islands",
    "521": "Palau",
    "525": "Indonesia",
    "526": "Indonesia",
    "533": "Malaysia",
    "538": "Philippines",
    "539": "Vietnam",
    "563": "Singapore",
    "566": "Malaysia",
    "567": "Brunei",

    # Africa
    "600": "South Africa",
    "601": "Angola",
    "602": "Benin",
    "603": "Mauritius",
    "604": "Morocco",
    "605": "Algeria",
    "606": "Madagascar",
    "607": "St Paul & Amsterdam Is",
    "609": "Burundi",
    "610": "Benin",
    "611": "Botswana",
    "612": "Central African Republic",
    "613": "Cameroon",
    "618": "Reunion",
    "619": "Djibouti",
    "620": "Eritrea",
    "621": "Ethiopia",
    "622": "Gabon",
    "624": "Ghana",
    "626": "Guinea-Bissau",
    "628": "Kenya",
    "629": "Lesotho",
    "630": "Liberia",
    "634": "Mali",
    "635": "Mozambique",
    "636": "Namibia",
    "637": "Niger",
    "638": "Nigeria",
    "639": "Rwanda",
    "641": "Senegal",
    "642": "Seychelles",
    "643": "Sierra Leone",
    "644": "Somalia",
    "645": "South Africa",
    "646": "Sudan",
    "647": "Tanzania",
    "648": "Togo",
    "649": "Tunisia",

    # South America
    "700": "Argentina",
    "710": "Chile",
    "720": "Venezuela",
    "730": "Panama",
    "740": "Guyana",
}


# ============================================================
# AIS FRAGMENT CACHE
# ============================================================

ais_fragments = {}

ais_fragments_lock = threading.Lock()


# ============================================================
# UTILITY
# ============================================================

def now_utc():
    return datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def get_db():
    conn = sqlite3.connect(
        DB_NAME,
        timeout=30,
        check_same_thread=False
    )

    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")

    return conn


def execute_with_retry(func, retries=5, delay=0.2):
    last_error = None

    for i in range(retries):
        try:
            return func()

        except sqlite3.OperationalError as e:
            last_error = e

            if "locked" in str(e).lower():
                time.sleep(delay * (i + 1))
                continue

            raise

    print(f"[DB ERROR] Max retry reached: {last_error}")

    return None


def clean_text(value):
    """
    Membersihkan string AIS.

    Contoh:
        'SHIP NAME     ' -> 'SHIP NAME'
        'VESSEL@@@@@@@' -> 'VESSEL'
        None -> None
    """

    if value is None:
        return None

    try:
        value = str(value)
    except Exception:
        return None

    value = value.replace("@", " ")
    value = value.replace("\x00", "")
    value = value.strip()

    if not value:
        return None

    return value


def clean_shipname(value):
    value = clean_text(value)

    if not value:
        return None

    # Beberapa decoder menghasilkan slash sebagai padding
    value = value.replace("/", " ").strip()

    if not value:
        return None

    return value


def to_float(value, default=None):
    try:
        if value is None:
            return default

        return float(value)

    except Exception:
        return default


def to_int(value, default=None):
    try:
        if value is None:
            return default

        return int(value)

    except Exception:
        return default


def lookup_nav_status(code):
    try:
        return NAV_STATUS.get(
            int(code),
            "Unknown"
        )
    except Exception:
        return "Unknown"


def lookup_ship_type(code):
    try:
        return SHIP_TYPE.get(
            int(code),
            "Unknown"
        )
    except Exception:
        return "Unknown"


def lookup_country(mmsi):
    try:
        mid = str(mmsi)[:3]

        return COUNTRY.get(
            mid,
            "UNKNOWN"
        )

    except Exception:
        return "UNKNOWN"


# ============================================================
# CONFIG LOADERS
# ============================================================

def load_adsb_sources():
    return [
        s.strip()
        for s in config.get(
            "adsb",
            "sources",
            fallback=""
        ).split(",")
        if s.strip()
    ]


def load_radio_sources():
    result = []

    for s in config.get(
        "radio",
        "sources",
        fallback=""
    ).split(","):

        s = s.strip()

        if not s:
            continue

        if "|" in s:
            try:
                url, cred = s.split("|", 1)
                username, password = cred.split(":", 1)

                result.append(
                    (
                        url.strip(),
                        username.strip(),
                        password.strip()
                    )
                )

            except Exception as e:
                print(
                    f"[RADIO CONFIG ERROR] "
                    f"{s} -> {e}"
                )

        else:
            result.append(
                (
                    s,
                    None,
                    None
                )
            )

    return result


def load_ais_sources():
    result = []

    if not config.has_section("ais"):
        print(
            "[AIS] Section [ais] "
            "tidak ditemukan"
        )

        return result

    for s in config.get(
        "ais",
        "sources",
        fallback=""
    ).split(","):

        s = s.strip()

        if not s:
            continue

        try:
            ip, port = s.rsplit(":", 1)

            result.append(
                (
                    ip.strip(),
                    int(port.strip())
                )
            )

        except Exception as e:
            print(
                f"[AIS CONFIG ERROR] "
                f"{s} -> {e}"
            )

    return result


# ============================================================
# DATABASE SCHEMA CHECK
# ============================================================

def ensure_database():
    """
    Tidak mengubah schema existing.
    Hanya memastikan tabel utama tersedia.

    Jika tabel sudah dibuat oleh aplikasi lama,
    CREATE TABLE IF NOT EXISTS tidak mengganggu.
    """

    def _exec():
        with db_lock:
            conn = get_db()
            cur = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS ships (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mmsi TEXT UNIQUE,
                    shipname TEXT,
                    lat REAL,
                    lon REAL,
                    speed REAL,
                    course REAL,
                    heading REAL,
                    ship_type TEXT,
                    nav_stat TEXT,
                    country TEXT,
                    last_seen TEXT
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS updateships (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mmsi TEXT UNIQUE,
                    message_type INTEGER,
                    shipname TEXT,
                    ship_type TEXT,
                    nav_stat TEXT,
                    country TEXT,
                    destination TEXT,
                    lat REAL,
                    lon REAL,
                    speed REAL,
                    course REAL,
                    heading REAL,
                    callsign TEXT,
                    imo TEXT,
                    eta TEXT,
                    last_seen TEXT
                )
            """)

            conn.commit()
            conn.close()

    execute_with_retry(_exec)


# ============================================================
# AIS PARSING
# ============================================================

def get_data_value(data, *names):
    """
    Ambil field berdasarkan beberapa kemungkinan nama.

    pyais bisa memiliki variasi field tergantung
    message type / versi library.
    """

    for name in names:
        if name in data:
            value = data.get(name)

            if value is not None:
                return value

    return None


def parse_ais_sentence(sentence):
    """
    Decode satu AIS message yang SUDAH lengkap.

    Penting:
    Fungsi ini tidak menangani fragment.
    Fragment ditangani oleh process_ais_line().
    """

    try:
        if not sentence:
            return None

        sentence = sentence.strip()

        if not sentence.startswith("!"):
            return None

        # pyais decode
        msg = decode(
            sentence,
            error_if_checksum_invalid=False
        )

        data = msg.asdict()

        msg_type = to_int(
            get_data_value(
                data,
                "msg_type",
                "message_type"
            )
        )

        if msg_type not in VALID_MSG_TYPES:
            return None

        mmsi = get_data_value(
            data,
            "mmsi"
        )

        if not mmsi:
            return None

        mmsi = str(mmsi)

        # ----------------------------------------------------
        # POSITION
        # ----------------------------------------------------

        lat = get_data_value(
            data,
            "lat",
            "latitude"
        )

        lon = get_data_value(
            data,
            "lon",
            "longitude"
        )

        speed = get_data_value(
            data,
            "speed",
            "sog"
        )

        course = get_data_value(
            data,
            "course",
            "cog"
        )

        heading = get_data_value(
            data,
            "heading",
            "true_heading"
        )

        # ----------------------------------------------------
        # STATIC
        # ----------------------------------------------------

        shipname = get_data_value(
            data,
            "shipname",
            "ship_name",
            "name"
        )

        callsign = get_data_value(
            data,
            "callsign",
            "call_sign"
        )

        destination = get_data_value(
            data,
            "destination"
        )

        imo = get_data_value(
            data,
            "imo"
        )

        ship_type = get_data_value(
            data,
            "ship_type",
            "shiptype"
        )

        status = get_data_value(
            data,
            "status",
            "nav_status"
        )

        # ----------------------------------------------------
        # TYPE 24 SPECIAL HANDLING
        # ----------------------------------------------------

        part = get_data_value(
            data,
            "part",
            "part_number"
        )

        # pyais biasanya memberikan:
        #
        # Type 24 Part A:
        #   shipname
        #
        # Type 24 Part B:
        #   callsign
        #   ship_type
        #   vendor_id
        #   dim_to_bow
        #   dim_to_stern
        #   dim_to_port
        #   dim_to_starboard
        #
        # Jangan menghapus data hanya karena field tertentu None.

        if msg_type == 24:

            if shipname is not None:
                shipname = clean_shipname(shipname)

            if callsign is not None:
                callsign = clean_text(callsign)

            if destination is not None:
                destination = clean_text(destination)

        else:

            shipname = clean_shipname(shipname)
            callsign = clean_text(callsign)
            destination = clean_text(destination)

        # ----------------------------------------------------
        # RETURN
        # ----------------------------------------------------

        result = {
            "mmsi": mmsi,
            "message_type": msg_type,

            "lat": to_float(lat),
            "lon": to_float(lon),

            "speed": to_float(speed),
            "course": to_float(course),
            "heading": to_float(heading),

            "shipname": shipname,
            "callsign": callsign,
            "destination": destination,

            "country": lookup_country(mmsi),

            "imo": imo,
            "ship_type": to_int(ship_type),
            "status": to_int(status),

            "part": part,

            "last_seen": now_utc(),

            # Simpan raw data untuk debugging
            "_raw": data,
        }

        return result

    except Exception as e:
        print(
            f"[AIS PARSE ERROR] "
            f"{sentence} -> {e}"
        )

        return None


# ============================================================
# AIS FRAGMENT HANDLER
# ============================================================

def process_ais_line(line):
    """
    Memproses:
        !AIVDM,1,1,...
        !AIVDM,2,1,...
        !AIVDM,2,2,...

    Fragment AIS digabung berdasarkan sequence ID.
    """

    line = line.strip()

    if not line:
        return None

    if not line.startswith("!"):
        return None

    try:
        parts = line.split(",")

        # Minimum:
        # !AIVDM
        # fragment count
        # fragment number
        # sequence id
        # channel
        # payload
        # fill bits

        if len(parts) < 7:
            return None

        fragment_count = int(parts[1])
        fragment_number = int(parts[2])
        sequence_id = parts[3].strip()
        channel = parts[4].strip()

        # ----------------------------------------------------
        # SINGLE MESSAGE
        # ----------------------------------------------------

        if fragment_count == 1:
            return parse_ais_sentence(line)

        # ----------------------------------------------------
        # MULTI FRAGMENT
        # ----------------------------------------------------

        # Sequence ID kadang kosong.
        # Jika kosong, gunakan channel sebagai bagian key.
        if not sequence_id:
            sequence_id = f"NOSEQ_{channel}"

        key = (
            f"{channel}:"
            f"{sequence_id}"
        )

        now = time.time()

        with ais_fragments_lock:

            # Cleanup fragment lama
            expired = []

            for k, item in ais_fragments.items():
                if (
                    now - item["time"]
                    > FRAGMENT_TIMEOUT
                ):
                    expired.append(k)

            for k in expired:
                del ais_fragments[k]

            # Buat buffer
            if key not in ais_fragments:
                ais_fragments[key] = {
                    "time": now,
                    "total": fragment_count,
                    "parts": {}
                }

            item = ais_fragments[key]

            item["time"] = now
            item["total"] = fragment_count

            item["parts"][fragment_number] = line

            # Belum lengkap
            if len(item["parts"]) < fragment_count:
                return None

            # ------------------------------------------------
            # Semua fragment sudah ada
            # ------------------------------------------------

            complete_parts = []

            for n in range(
                1,
                fragment_count + 1
            ):

                if n not in item["parts"]:
                    return None

                complete_parts.append(
                    item["parts"][n]
                )

            del ais_fragments[key]

        # ----------------------------------------------------
        # Gabungkan payload
        #
        # Lebih aman membuat satu sentence baru
        # daripada mengirim dua sentence ke pyais.
        # ----------------------------------------------------

        try:
            first = complete_parts[0].split(",")

            last = complete_parts[-1].split(",")

            payloads = []

            for p in complete_parts:
                pparts = p.split(",")

                if len(pparts) >= 7:
                    payloads.append(
                        pparts[5]
                    )

            combined_payload = "".join(
                payloads
            )

            fill_bits = last[6]

            prefix = first[0]

            # pyais membutuhkan sentence valid.
            # Kita buat checksum baru.

            base = (
                f"{prefix},1,1,,A,"
                f"{combined_payload},"
                f"{fill_bits}"
            )

            checksum = 0

            for char in base[1:]:
                checksum ^= ord(char)

            sentence = (
                f"{base}*"
                f"{checksum:02X}"
            )

            return parse_ais_sentence(
                sentence
            )

        except Exception as e:
            print(
                f"[AIS FRAGMENT ERROR] "
                f"{key} -> {e}"
            )

            return None

    except Exception as e:
        print(
            f"[AIS LINE ERROR] "
            f"{line} -> {e}"
        )

        return None


# ============================================================
# UPSERT SNAPSHOT
# ============================================================

def upsert_snapshot(info):
    """
    updateships = latest complete/partial AIS information.

    Yang penting:
    field yang None TIDAK menimpa nilai lama.

    Ini sangat penting untuk Type 1 + Type 5.

    Contoh:

        Type 1:
            MMSI 123
            lat/lon ada
            shipname None

        Type 5:
            MMSI 123
            shipname = EVER GIVEN
            lat/lon None

    Database akhirnya:
        lat/lon tetap ada
        shipname = EVER GIVEN
    """

    if not info:
        return

    mmsi = info.get("mmsi")

    if not mmsi:
        return

    def _exec():

        with db_lock:

            conn = get_db()
            cur = conn.cursor()

            cur.execute(
                """
                SELECT
                    message_type,
                    shipname,
                    ship_type,
                    nav_stat,
                    country,
                    destination,
                    lat,
                    lon,
                    speed,
                    course,
                    heading,
                    callsign,
                    imo,
                    eta,
                    last_seen
                FROM updateships
                WHERE mmsi=?
                """,
                (mmsi,)
            )

            old = cur.fetchone()

            # ------------------------------------------------
            # Existing data
            # ------------------------------------------------

            if old:

                (
                    old_message_type,
                    old_shipname,
                    old_ship_type,
                    old_nav_stat,
                    old_country,
                    old_destination,
                    old_lat,
                    old_lon,
                    old_speed,
                    old_course,
                    old_heading,
                    old_callsign,
                    old_imo,
                    old_eta,
                    old_last_seen,
                ) = old

            else:

                (
                    old_message_type,
                    old_shipname,
                    old_ship_type,
                    old_nav_stat,
                    old_country,
                    old_destination,
                    old_lat,
                    old_lon,
                    old_speed,
                    old_course,
                    old_heading,
                    old_callsign,
                    old_imo,
                    old_eta,
                    old_last_seen,
                ) = (
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )

            # ------------------------------------------------
            # New values
            # ------------------------------------------------

            new_shipname = (
                info.get("shipname")
                or old_shipname
            )

            ship_type_code = info.get(
                "ship_type"
            )

            if ship_type_code is not None:
                new_ship_type = lookup_ship_type(
                    ship_type_code
                )
            else:
                new_ship_type = old_ship_type

            status_code = info.get(
                "status"
            )

            if status_code is not None:
                new_nav_stat = lookup_nav_status(
                    status_code
                )
            else:
                new_nav_stat = old_nav_stat

            new_country = (
                info.get("country")
                if info.get("country")
                and info.get("country") != "UNKNOWN"
                else old_country
            )

            new_destination = (
                info.get("destination")
                or old_destination
            )

            new_lat = (
                info.get("lat")
                if info.get("lat") is not None
                else old_lat
            )

            new_lon = (
                info.get("lon")
                if info.get("lon") is not None
                else old_lon
            )

            new_speed = (
                info.get("speed")
                if info.get("speed") is not None
                else old_speed
            )

            new_course = (
                info.get("course")
                if info.get("course") is not None
                else old_course
            )

            new_heading = (
                info.get("heading")
                if info.get("heading") is not None
                else old_heading
            )

            new_callsign = (
                info.get("callsign")
                or old_callsign
            )

            new_imo = (
                info.get("imo")
                or old_imo
            )

            new_last_seen = (
                info.get("last_seen")
                or old_last_seen
            )

            new_message_type = (
                info.get("message_type")
                or old_message_type
            )

            # ------------------------------------------------
            # UPSERT
            # ------------------------------------------------

            if old:

                cur.execute(
                    """
                    UPDATE updateships
                    SET
                        message_type=?,
                        shipname=?,
                        ship_type=?,
                        nav_stat=?,
                        country=?,
                        destination=?,
                        lat=?,
                        lon=?,
                        speed=?,
                        course=?,
                        heading=?,
                        callsign=?,
                        imo=?,
                        eta=?,
                        last_seen=?
                    WHERE mmsi=?
                    """,
                    (
                        new_message_type,
                        new_shipname,
                        new_ship_type,
                        new_nav_stat,
                        new_country,
                        new_destination,
                        new_lat,
                        new_lon,
                        new_speed,
                        new_course,
                        new_heading,
                        new_callsign,
                        new_imo,
                        old_eta,
                        new_last_seen,
                        mmsi,
                    )
                )

            else:

                cur.execute(
                    """
                    INSERT INTO updateships (
                        mmsi,
                        message_type,
                        shipname,
                        ship_type,
                        nav_stat,
                        country,
                        destination,
                        lat,
                        lon,
                        speed,
                        course,
                        heading,
                        callsign,
                        imo,
                        eta,
                        last_seen
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """,
                    (
                        mmsi,
                        new_message_type,
                        new_shipname,
                        new_ship_type,
                        new_nav_stat,
                        new_country,
                        new_destination,
                        new_lat,
                        new_lon,
                        new_speed,
                        new_course,
                        new_heading,
                        new_callsign,
                        new_imo,
                        old_eta,
                        new_last_seen,
                    )
                )

            conn.commit()
            conn.close()

    execute_with_retry(_exec)


# ============================================================
# SAVE AIS
# ============================================================

def save_ais(info):
    """
    Table ships = current/latest vessel position.

    Static information seperti nama kapal juga dipertahankan.
    """

    if not info:
        return

    mmsi = info.get("mmsi")

    if not mmsi:
        return

    def _exec():

        with db_lock:

            conn = get_db()
            cur = conn.cursor()

            cur.execute(
                """
                SELECT
                    shipname,
                    lat,
                    lon,
                    speed,
                    course,
                    heading,
                    ship_type,
                    nav_stat,
                    country,
                    last_seen
                FROM ships
                WHERE mmsi=?
                """,
                (mmsi,)
            )

            old = cur.fetchone()

            if old:

                (
                    old_shipname,
                    old_lat,
                    old_lon,
                    old_speed,
                    old_course,
                    old_heading,
                    old_ship_type,
                    old_nav_stat,
                    old_country,
                    old_last_seen,
                ) = old

            else:

                (
                    old_shipname,
                    old_lat,
                    old_lon,
                    old_speed,
                    old_course,
                    old_heading,
                    old_ship_type,
                    old_nav_stat,
                    old_country,
                    old_last_seen,
                ) = (
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                )

            # ------------------------------------------------
            # Merge
            # ------------------------------------------------

            shipname = (
                info.get("shipname")
                or old_shipname
            )

            lat = (
                info.get("lat")
                if info.get("lat") is not None
                else old_lat
            )

            lon = (
                info.get("lon")
                if info.get("lon") is not None
                else old_lon
            )

            speed = (
                info.get("speed")
                if info.get("speed") is not None
                else old_speed
            )

            course = (
                info.get("course")
                if info.get("course") is not None
                else old_course
            )

            heading = (
                info.get("heading")
                if info.get("heading") is not None
                else old_heading
            )

            if info.get("ship_type") is not None:

                ship_type = lookup_ship_type(
                    info.get("ship_type")
                )

            else:
                ship_type = old_ship_type

            if info.get("status") is not None:

                nav_stat = lookup_nav_status(
                    info.get("status")
                )

            else:
                nav_stat = old_nav_stat

            country = (
                info.get("country")
                if info.get("country")
                and info.get("country") != "UNKNOWN"
                else old_country
            )

            last_seen = (
                info.get("last_seen")
                or old_last_seen
            )

            # ------------------------------------------------
            # INSERT / UPDATE
            # ------------------------------------------------

            if old:

                cur.execute(
                    """
                    UPDATE ships
                    SET
                        shipname=?,
                        lat=?,
                        lon=?,
                        speed=?,
                        course=?,
                        heading=?,
                        ship_type=?,
                        nav_stat=?,
                        country=?,
                        last_seen=?
                    WHERE mmsi=?
                    """,
                    (
                        shipname,
                        lat,
                        lon,
                        speed,
                        course,
                        heading,
                        ship_type,
                        nav_stat,
                        country,
                        last_seen,
                        mmsi,
                    )
                )

            else:

                cur.execute(
                    """
                    INSERT INTO ships (
                        mmsi,
                        shipname,
                        lat,
                        lon,
                        speed,
                        course,
                        heading,
                        ship_type,
                        nav_stat,
                        country,
                        last_seen
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?
                    )
                    """,
                    (
                        mmsi,
                        shipname,
                        lat,
                        lon,
                        speed,
                        course,
                        heading,
                        ship_type,
                        nav_stat,
                        country,
                        last_seen,
                    )
                )

            conn.commit()
            conn.close()

    execute_with_retry(_exec)


# ============================================================
# ADS-B
# ============================================================

def upsert_aircraft(data):

    def _exec():

        with db_lock:

            conn = get_db()
            cur = conn.cursor()

            cur.execute(
                """
                INSERT INTO flight (
                    hex,
                    flight,
                    lat,
                    lon,
                    altitude,
                    speed,
                    track,
                    vert_rate,
                    squawk,
                    category,
                    messages,
                    seen,
                    seen_pos,
                    rssi,
                    timestamp
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?,
                    CURRENT_TIMESTAMP
                )
                """,
                (
                    data.get("hex"),
                    data.get("flight"),
                    data.get("lat"),
                    data.get("lon"),
                    data.get("altitude"),
                    data.get("speed"),
                    data.get("track"),
                    data.get("vert_rate"),
                    data.get("squawk"),
                    data.get("category"),
                    data.get("messages"),
                    data.get("seen"),
                    data.get("seen_pos"),
                    data.get("rssi"),
                )
            )

            flight = data.get("flight")

            if not flight:
                conn.commit()
                conn.close()
                return

            cur.execute(
                """
                SELECT id
                FROM updateflight
                WHERE flight=?
                """,
                (flight,)
            )

            exists = cur.fetchone()

            values = (
                data.get("hex"),
                data.get("lat"),
                data.get("lon"),
                data.get("altitude"),
                data.get("speed"),
                data.get("track"),
                data.get("vert_rate"),
                data.get("squawk"),
                data.get("category"),
                data.get("messages"),
                data.get("seen"),
                data.get("seen_pos"),
                data.get("rssi"),
            )

            if exists:

                cur.execute(
                    """
                    UPDATE updateflight
                    SET
                        hex=?,
                        lat=?,
                        lon=?,
                        altitude=?,
                        speed=?,
                        track=?,
                        vert_rate=?,
                        squawk=?,
                        category=?,
                        messages=?,
                        seen=?,
                        seen_pos=?,
                        rssi=?,
                        timestamp=CURRENT_TIMESTAMP
                    WHERE flight=?
                    """,
                    (
                        *values,
                        flight,
                    )
                )

            else:

                cur.execute(
                    """
                    INSERT INTO updateflight (
                        hex,
                        flight,
                        lat,
                        lon,
                        altitude,
                        speed,
                        track,
                        vert_rate,
                        squawk,
                        category,
                        messages,
                        seen,
                        seen_pos,
                        rssi,
                        timestamp
                    )
                    VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?,
                        CURRENT_TIMESTAMP
                    )
                    """,
                    (
                        data.get("hex"),
                        flight,
                        data.get("lat"),
                        data.get("lon"),
                        data.get("altitude"),
                        data.get("speed"),
                        data.get("track"),
                        data.get("vert_rate"),
                        data.get("squawk"),
                        data.get("category"),
                        data.get("messages"),
                        data.get("seen"),
                        data.get("seen_pos"),
                        data.get("rssi"),
                    )
                )

            conn.commit()
            conn.close()

    execute_with_retry(_exec)


def adsb_worker(url):

    while True:

        try:

            response = requests.get(
                url,
                timeout=3
            )

            response.raise_for_status()

            data = response.json()

            aircraft = data.get(
                "aircraft",
                []
            )

            if not isinstance(
                aircraft,
                list
            ):
                time.sleep(1)
                continue

            for ac in aircraft:

                if (
                    isinstance(ac, dict)
                    and ac.get("flight")
                ):
                    upsert_aircraft(ac)

        except Exception as e:

            print(
                f"[ADSB ERROR] "
                f"{url} {e}"
            )

        time.sleep(0.5)


def adsb_loop():

    while True:

        for url in load_adsb_sources():

            if url not in adsb_workers:

                t = threading.Thread(
                    target=adsb_worker,
                    args=(url,),
                    daemon=True
                )

                t.start()

                adsb_workers[url] = t

        time.sleep(5)


# ============================================================
# RADIO
# ============================================================

def upsert_radio(data):

    try:

        with db_lock:

            conn = get_db()
            cur = conn.cursor()

            address = data.get(
                "address"
            )

            lat = to_float(
                data.get("latitude"),
                0.0
            )

            lon = to_float(
                data.get("longitude"),
                0.0
            )

            speed = to_float(
                data.get("speed"),
                0.0
            )

            ts = now_utc()

            cur.execute(
                """
                INSERT INTO radio (
                    address,
                    latitude,
                    longitude,
                    speed,
                    timestamp
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    address,
                    lat,
                    lon,
                    speed,
                    ts
                )
            )

            cur.execute(
                """
                SELECT id
                FROM updateradio
                WHERE address=?
                """,
                (address,)
            )

            exists = cur.fetchone()

            if exists:

                cur.execute(
                    """
                    UPDATE updateradio
                    SET
                        latitude=?,
                        longitude=?,
                        speed=?,
                        timestamp=?
                    WHERE address=?
                    """,
                    (
                        lat,
                        lon,
                        speed,
                        ts,
                        address
                    )
                )

            else:

                cur.execute(
                    """
                    INSERT INTO updateradio (
                        address,
                        latitude,
                        longitude,
                        speed,
                        timestamp
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        address,
                        lat,
                        lon,
                        speed,
                        ts
                    )
                )

            conn.commit()
            conn.close()

    except Exception as e:

        print(
            "[RADIO ERROR]",
            e
        )


def radio_worker(
    url,
    username=None,
    password=None
):

    while True:

        try:

            if username and password:

                response = requests.get(
                    url,
                    auth=HTTPBasicAuth(
                        username,
                        password
                    ),
                    timeout=5,
                    verify=False
                )

            else:

                response = requests.get(
                    url,
                    timeout=5,
                    verify=False
                )

            if response.status_code != 200:

                print(
                    f"[RADIO HTTP ERROR] "
                    f"{url} "
                    f"{response.status_code}"
                )

                time.sleep(2)
                continue

            data = response.json()

            records = data.get(
                "records",
                []
            )

            if not isinstance(
                records,
                list
            ):

                print(
                    "[RADIO FORMAT ERROR] "
                    "records bukan list"
                )

                time.sleep(2)
                continue

            for record in records:

                if isinstance(
                    record,
                    dict
                ):
                    upsert_radio(
                        record
                    )

        except Exception as e:

            print(
                f"[RADIO ERROR] "
                f"{url} {e}"
            )

        time.sleep(1)


def radio_loop():

    while True:

        for (
            url,
            username,
            password
        ) in load_radio_sources():

            key = (
                f"{url}|"
                f"{username}"
            )

            if key not in radio_workers:

                t = threading.Thread(
                    target=radio_worker,
                    args=(
                        url,
                        username,
                        password
                    ),
                    daemon=True
                )

                t.start()

                radio_workers[key] = t

        time.sleep(5)


# ============================================================
# AIS TCP WORKER
# ============================================================

def tcp_worker(ip, port):

    while True:

        client = None

        try:

            client = socket.socket(
                socket.AF_INET,
                socket.SOCK_STREAM
            )

            client.settimeout(
                SOCKET_RECV_TIMEOUT
            )

            client.setsockopt(
                socket.SOL_SOCKET,
                socket.SO_KEEPALIVE,
                1
            )

            client.connect(
                (ip, port)
            )

            print(
                f"[AIS CONNECTED] "
                f"{ip}:{port}"
            )

            buffer = ""

            while True:

                try:

                    chunk = client.recv(
                        BUFFER_SIZE
                    )

                except socket.timeout:

                    # Tidak langsung disconnect.
                    # Cek lagi koneksi.
                    continue

                if not chunk:

                    print(
                        f"[AIS DISCONNECTED] "
                        f"{ip}:{port}"
                    )

                    break

                buffer += chunk.decode(
                    "ascii",
                    errors="ignore"
                )

                # ------------------------------------------------
                # Handle CRLF / LF
                # ------------------------------------------------

                while "\n" in buffer:

                    line, buffer = (
                        buffer.split(
                            "\n",
                            1
                        )
                    )

                    line = line.strip()

                    if not line:
                        continue

                    # AIS VDM/VDO
                    if (
                        line.startswith(
                            "!AIVDM"
                        )
                        or line.startswith(
                            "!AIVDO"
                        )
                    ):

                        info = process_ais_line(
                            line
                        )

                        if info:

                            # DEBUG static message
                            if (
                                info.get(
                                    "message_type"
                                )
                                in STATIC_MSG_TYPES
                            ):

                                print(
                                    "[AIS STATIC]",
                                    info.get(
                                        "message_type"
                                    ),
                                    info.get(
                                        "mmsi"
                                    ),
                                    repr(
                                        info.get(
                                            "shipname"
                                        )
                                    ),
                                    repr(
                                        info.get(
                                            "callsign"
                                        )
                                    )
                                )

                            save_ais(info)

                            upsert_snapshot(info)

        except Exception as e:

            print(
                f"[AIS ERROR] "
                f"{ip}:{port} "
                f"{e}"
            )

        finally:

            if client:

                try:
                    client.close()
                except Exception:
                    pass

        time.sleep(5)


# ============================================================
# AIS LOOP
# ============================================================

def fetch_ais_loop():

    while True:

        for (
            ip,
            port
        ) in load_ais_sources():

            key = (
                f"{ip}:{port}"
            )

            if key not in ais_workers:

                t = threading.Thread(
                    target=tcp_worker,
                    args=(
                        ip,
                        port
                    ),
                    daemon=True
                )

                t.start()

                ais_workers[key] = t

                print(
                    f"[AIS] "
                    f"Worker started "
                    f"{key}"
                )

        time.sleep(5)


# ============================================================
# DEBUG AIS
# ============================================================

@app.route("/ais")
def ais_status():

    def _exec():

        with db_lock:

            conn = get_db()

            cur = conn.cursor()

            cur.execute(
                """
                SELECT
                    mmsi,
                    message_type,
                    shipname,
                    ship_type,
                    nav_stat,
                    country,
                    destination,
                    lat,
                    lon,
                    speed,
                    course,
                    heading,
                    callsign,
                    imo,
                    last_seen
                FROM updateships
                ORDER BY last_seen DESC
                LIMIT 100
                """
            )

            rows = cur.fetchall()

            conn.close()

            return rows

    rows = execute_with_retry(
        _exec
    )

    result = []

    for row in rows or []:

        result.append(
            {
                "mmsi": row[0],
                "message_type": row[1],
                "shipname": row[2],
                "ship_type": row[3],
                "nav_stat": row[4],
                "country": row[5],
                "destination": row[6],
                "lat": row[7],
                "lon": row[8],
                "speed": row[9],
                "course": row[10],
                "heading": row[11],
                "callsign": row[12],
                "imo": row[13],
                "last_seen": row[14],
            }
        )

    return result


@app.route("/health")
def health():

    return {
        "status": "ok",
        "time": now_utc(),
        "ais_workers": len(ais_workers),
        "adsb_workers": len(adsb_workers),
        "radio_workers": len(radio_workers),
    }


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    print(
        "=========================================="
    )

    print(
        " AIS / ADS-B / RADIO COLLECTOR"
    )

    print(
        "=========================================="
    )

    print(
        f"[CONFIG] {CONFIG_PATH}"
    )

    print(
        f"[DATABASE] {DB_NAME}"
    )

    ensure_database()

    # --------------------------------------------------------
    # ADS-B
    # --------------------------------------------------------

    threading.Thread(
        target=adsb_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # RADIO
    # --------------------------------------------------------

    threading.Thread(
        target=radio_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # AIS
    # --------------------------------------------------------

    threading.Thread(
        target=fetch_ais_loop,
        daemon=True
    ).start()

    print(
        "[SYSTEM] All workers started"
    )

    app.run(
        host="0.0.0.0",
        port=5001,
        threaded=True
    )
