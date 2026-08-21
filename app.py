import threading
import time
from datetime import datetime
from typing import List
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pymysql

DB_HOST = "199.14.10.150"
DB_USER = "datawuser"
DB_PASS = "datawpassword"
DB_NAME = "Dataw"

ENTIDADES_MAP = {
    "1005": "BCP",
    "0009": "BMSC",
    "0008": "BNB",
    "0004": "BISA",
    "5006": "ECOFUTURO",
    "9065": "DIACONIA"
}

TABLAS_ESPECIFICAS = [
    ("mov_telecel", "TELECEL"),
    ("mov_multivision", "MULTIVISION / TIGO HOME"),
    ("mov_nuevatel", "VIVA / NUEVATEL"),
    ("mov_axs", "AXS"),
    ("mov_tuves", "TUVES"),
    ("mov_entel", "ENTEL"),
    ("mov_entelpre", "ENTEL PREPAGO")
]

ORIGENES_PERMITIDOS = "('APWS', 'APWG', 'APWA', 'APWY', 'APWB', 'APMO', 'APTE', 'MCQN', 'LUKA')"
CODS_ENTIDADES_MIX = "('1005', '0009', '0008', '0004', '5006', '9065', 1005, 9, 8, 4, 5006, 9065, 20, 21)"

MAPPING_COD_CLEAN = {
    "1005": "1005", "0020": "1005", "20": "1005",
    "0008": "0008", "8": "0008",
    "0009": "0009", "9": "0009", "0021": "0009", "21": "0009",
    "0004": "0004", "4": "0004",
    "5006": "5006",
    "9065": "9065"
}

def obtener_estructura_base():
    return {
        "resumen": {
            "total_transacciones": 0,
            "entidades_monitoreadas": 6,
            "alertas_activas": 0,
            "timestamp": "Actualizando..."
        },
        "entidades": [
            {
                "entidad": nombre,
                "cod_entidad": cod,
                "total_ultima_hora": 0,
                "alerta": False,
                "top_servicios": []
            }
            for cod, nombre in ENTIDADES_MAP.items()
        ]
    }

CACHE_MONITOREO = obtener_estructura_base()

def consultar_base_datos():
    global CACHE_MONITOREO
    try:
        conn = pymysql.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
            connect_timeout=5, read_timeout=10
        )
        cursor = conn.cursor()

        datos_entidades = {
            cod: {
                "entidad": nombre,
                "cod_entidad": cod,
                "total_ultima_hora": 0,
                "alerta": False,
                "servicios": {}, # serv_nombre -> {"total": int, "puntos": {min_str: count}}
                "ultima_fecha": None
            }
            for cod, nombre in ENTIDADES_MAP.items()
        }

        for tabla, serv_nombre in TABLAS_ESPECIFICAS:
            query = f"""
                SELECT 
                    CAST(cod_entidad AS CHAR) AS cod_raw,
                    DATE_FORMAT(hora, '%H:%i') AS min_str,
                    COUNT(*) AS cant,
                    MAX(ADDTIME(CAST(fecha AS DATETIME), hora)) AS max_f
                FROM {tabla}
                WHERE UPPER(TRIM(estado)) = 'P'
                  AND cod_entidad IN {CODS_ENTIDADES_MIX}
                  AND UPPER(TRIM(origen)) IN {ORIGENES_PERMITIDOS}
                  AND fecha >= CURDATE() - INTERVAL 1 DAY
                  AND ADDTIME(CAST(fecha AS DATETIME), hora) >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
                GROUP BY cod_raw, min_str;
            """
            try:
                cursor.execute(query)
                rows = cursor.fetchall()
                for cod_raw, min_str, cant, max_f in rows:
                    cod_str = str(cod_raw).zfill(4) if len(str(cod_raw)) < 4 else str(cod_raw)
                    cod_clean = MAPPING_COD_CLEAN.get(str(cod_raw), MAPPING_COD_CLEAN.get(cod_str, None))
                    
                    if cod_clean and cod_clean in datos_entidades:
                        ent = datos_entidades[cod_clean]
                        ent["total_ultima_hora"] += cant

                        if serv_nombre not in ent["servicios"]:
                            ent["servicios"][serv_nombre] = {"total": 0, "puntos": {}}
                        
                        ent["servicios"][serv_nombre]["total"] += cant
                        ent["servicios"][serv_nombre]["puntos"][min_str] = ent["servicios"][serv_nombre]["puntos"].get(min_str, 0) + cant

                        if ent["ultima_fecha"] is None or max_f > ent["ultima_fecha"]:
                            ent["ultima_fecha"] = max_f
            except Exception:
                continue

        conn.close()

        resultado_entidades = []
        total_general = 0
        alertas_activas = 0
        ahora = datetime.now()

        for cod_ent, info in datos_entidades.items():
            if info["ultima_fecha"] is None:
                info["alerta"] = True
            else:
                try:
                    fecha_val = info["ultima_fecha"]
                    fecha_dt = datetime.strptime(str(fecha_val)[:19], "%Y-%m-%d %H:%M:%S") if isinstance(fecha_val, str) else fecha_val
                    info["alerta"] = (ahora - fecha_dt).total_seconds() > 3600
                except Exception:
                    info["alerta"] = False

            if info["alerta"]:
                alertas_activas += 1

            total_general += info["total_ultima_hora"]

            # Formatear Top 5 servicios con sus puntos para la gráfica
            servicios_ordenados = sorted(info["servicios"].items(), key=lambda x: x[1]["total"], reverse=True)[:5]
            top_formatted = []
            for s_name, s_data in servicios_ordenados:
                puntos_list = [
                    {"minuto": m, "total": c} 
                    for m, c in sorted(s_data["puntos"].items())
                ]
                top_formatted.append({
                    "nombre": s_name,
                    "total": s_data["total"],
                    "puntos": puntos_list
                })

            resultado_entidades.append({
                "entidad": info["entidad"],
                "cod_entidad": cod_ent,
                "total_ultima_hora": info["total_ultima_hora"],
                "alerta": info["alerta"],
                "top_servicios": top_formatted
            })

        CACHE_MONITOREO = {
            "resumen": {
                "total_transacciones": total_general,
                "entidades_monitoreadas": len(resultado_entidades),
                "alertas_activas": alertas_activas,
                "timestamp": ahora.strftime("%Y-%m-%d %H:%M:%S")
            },
            "entidades": resultado_entidades
        }

    except Exception as e:
        print(f"Error actualizando métricas: {e}")

def background_worker():
    while True:
        consultar_base_datos()
        time.sleep(15) # Actualización rápida en segundo plano cada 15s

threading.Thread(target=background_worker, daemon=True).start()

app = FastAPI(title="API Monitoreo")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/v1/monitoreo")
def obtener_monitoreo_general():
    return CACHE_MONITOREO