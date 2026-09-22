from contextlib import asynccontextmanager
import threading
import time
from datetime import datetime, timedelta
from typing import List
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pymysql

DB_HOST = "199.14.10.150"
DB_USER = "datawuser"
DB_PASS = "datawpassword"
DB_NAME = "Dataw"

ENTIDADES_MAP = {
    "0304": "LUKA",
    "1005": "BCP",
    "1017": "BANCO SOL",
    "0035": "BANCO GANADERO",
    "0009": "BMSC",
    "0008": "BNB",
    "0004": "BISA",
    "5006": "ECOFUTURO",
    "9065": "DIACONIA"
}

TABLAS_ESPECIFICAS = [
    ("mov_telecel", "TELECEL"),
    ("mov_multivision", "TIGO HOME"),
    ("mov_nuevatel", "NUEVATEL"),
    ("mov_axs", "AXS"),
    ("mov_tuves", "TUVES"),
    ("mov_entelrec", "ENTEL SERVICIOS")
]

ORIGENES_PERMITIDOS = "('APWS', 'APWG', 'APWA', 'APWY', 'APWB', 'APMO', 'LUKA')"
CODS_ENTIDADES_MIX = "('1005', '0009', '0008', '0004', '5006', '9065', '1017', '0035', '0304')"

MAPPING_COD_CLEAN = {
    "1005": "1005", "0009": "0009", "0008": "0008",
    "0004": "0004", "5006": "5006", "9065": "9065",
    "0304": "0304", "1017": "1017", "0035": "0035"
}

def obtener_estructura_base():
    return {
        "resumen": {
            "total_transacciones": 0,
            "entidades_monitoreadas": len(ENTIDADES_MAP),
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

        cursor.execute("SELECT NOW()")
        db_now = cursor.fetchone()[0]
        ahora_dt = db_now if isinstance(db_now, datetime) else datetime.now()

        ultimos_60_min = [(ahora_dt - timedelta(minutes=i)).strftime("%H:%M") for i in range(59, -1, -1)]

        datos_entidades = {
            cod: {
                "entidad": nombre,
                "cod_entidad": cod,
                "total_ultima_hora": 0,
                "alerta": False,
                "servicios": {},
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
            except Exception as ex:
                print(f"Error consultando tabla {tabla}: {ex}")
                continue

        conn.close()

        resultado_entidades = []
        total_general = 0
        alertas_activas = 0

        for cod_ent, info in datos_entidades.items():
            if info["ultima_fecha"] is None:
                info["alerta"] = True
            else:
                try:
                    fecha_val = info["ultima_fecha"]
                    fecha_dt = datetime.strptime(str(fecha_val)[:19], "%Y-%m-%d %H:%M:%S") if isinstance(fecha_val, str) else fecha_val
                    info["alerta"] = (ahora_dt - fecha_dt).total_seconds() > 3600
                except Exception:
                    info["alerta"] = False

            if info["alerta"]:
                alertas_activas += 1

            total_general += info["total_ultima_hora"]

            servicios_ordenados = sorted(info["servicios"].items(), key=lambda x: x[1]["total"], reverse=True)[:5]
            top_formatted = []
            
            for s_name, s_data in servicios_ordenados:
                puntos_list = []
                for m in ultimos_60_min:
                    val = s_data["puntos"].get(m, 0)
                    puntos_list.append({
                        "minuto": m, "hora": m, "time": m, "x": m,
                        "total": val, "cant": val, "cantidad": val,
                        "count": val, "y": val, "valor": val
                    })

                top_formatted.append({
                    "nombre": s_name,
                    "total": s_data["total"],
                    "cant": s_data["total"],
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
                "timestamp": ahora_dt.strftime("%Y-%m-%d %H:%M:%S")
            },
            "entidades": resultado_entidades
        }

    except Exception as e:
        print(f"Error actualizando métricas: {e}")

def background_worker():
    while True:
        consultar_base_datos()
        time.sleep(15)

@asynccontextmanager
async def lifespan(app: FastAPI):
    thread = threading.Thread(target=background_worker, daemon=True)
    thread.start()
    yield

app = FastAPI(title="API Monitoreo", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/v1/monitoreo")
def obtener_monitoreo_general():
    return CACHE_MONITOREO

@app.get("/api/v1/alertas-premium")
def get_alertas_premium():
    canales = [
        {"nombre": "TIGO", "tabla": "mov_telecel"},
        {"nombre": "TIGO HOME", "tabla": "mov_multivision"},
        {"nombre": "ENTEL RECARGA", "tabla": "mov_entelrec"},
        {"nombre": "NUEVATEL", "tabla": "mov_nuevatel"},
        {"nombre": "EPSAS", "tabla": "mov_epsas"}
    ]
    
    ORIGENES_DIGITALES_PREMIUM = "('APWS', 'LUKA', 'APMO', 'APWB', 'APWY', 'APWA')"
    
    query = " UNION ALL ".join([
        f"SELECT '{c['nombre']}' AS canal, '{c['tabla']}' AS tabla, "
        f"COALESCE(COUNT(*), 0) AS tx_count "
        f"FROM {c['tabla']} "
        f"WHERE fecha >= CURDATE() - INTERVAL 1 DAY "
        f"  AND ADDTIME(CAST(fecha AS DATETIME), hora) >= DATE_SUB(NOW(), INTERVAL 10 MINUTE) "
        f"  AND UPPER(TRIM(estado)) = 'P' "
        f"  AND UPPER(TRIM(origen)) IN {ORIGENES_DIGITALES_PREMIUM}"
        for c in canales
    ])
    
    respuesta = []
    
    try:
        conn = pymysql.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
            connect_timeout=5, read_timeout=10,
            cursorclass=pymysql.cursors.DictCursor
        )
        
        with conn.cursor() as cursor:
            cursor.execute(query)
            resultados = cursor.fetchall()
            
            for row in resultados:
                cant = int(row["tx_count"]) if row["tx_count"] is not None else 0
                
                respuesta.append({
                    "canal": row["canal"],
                    "tabla": row["tabla"],
                    "tx_count": cant,
                    "tx_5min": cant,
                    "alerta": bool(cant == 0)
                })
                
        conn.close()
        
    except Exception as e:
        print(f"Error consultando alertas premium: {e}")
        return [
            {"canal": c["nombre"], "tabla": c["tabla"], "tx_count": 0, "tx_5min": 0, "alerta": True}
            for c in canales
        ]
        
    return respuesta

# ------------------------------------------------------------------
# NUEVO ENDPOINT OPTIMIZADO: MONITOREO CLIENTES (100+)
# ------------------------------------------------------------------
@app.get("/api/v1/monitoreo-clientes")
def get_monitoreo_clientes():
    resultado_clientes = []
    
    TABLAS_EXCLUIDAS = [
        "mov_rentdig", 
        "mov_sipgp", 
        "mov_unibienes", 
        "mov_bja"
    ]

    try:
        conn = pymysql.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
            connect_timeout=5, read_timeout=10, cursorclass=pymysql.cursors.DictCursor
        )
        cursor = conn.cursor()

        cursor.execute("SELECT NOW() AS ahora")
        db_now = cursor.fetchone()["ahora"]
        ahora_dt = db_now if isinstance(db_now, datetime) else datetime.now()

        # 📌 CORRECCIÓN: 'cliente' en singular
        cursor.execute("SELECT cod_cliente, descripcion, tabla FROM cliente WHERE UPPER(TRIM(estado)) = 'V'")
        clientes_vigentes = cursor.fetchall()

        for cli in clientes_vigentes:
            nombre_cli = cli.get("descripcion") or cli.get("cod_cliente") or "Sin Nombre"
            tabla_raw = str(cli.get("tabla") or "").strip().lower()

            if not tabla_raw:
                continue

            tabla_cli = tabla_raw if tabla_raw.startswith("mov_") else f"mov_{tabla_raw}"

            if tabla_cli in TABLAS_EXCLUIDAS:
                resultado_clientes.append({
                    "cod_cliente": str(cli["cod_cliente"]),
                    "descripcion": str(nombre_cli),
                    "tabla": tabla_cli,
                    "ultima_hora": "--:--:--",
                    "tx_hoy": 0,
                    "alerta": False,
                    "mensaje": "OMITIDO POR TAMAÑO"
                })
                continue

            # Consulta hiper-optimizada con LIMIT 1
            query_cli = f"""
                SELECT DATE_FORMAT(hora, '%H:%i:%s') AS ultima_hora
                FROM {tabla_cli}
                WHERE fecha = CURDATE() AND UPPER(TRIM(estado)) = 'P'
                ORDER BY hora DESC
                LIMIT 1
            """

            try:
                cursor.execute(query_cli)
                row = cursor.fetchone()
                hora_str = row["ultima_hora"] if row and row["ultima_hora"] else None

                alerta = False
                mensaje_estado = "OK"

                if not hora_str:
                    alerta = True
                    mensaje_estado = "SIN TRANSACCIONES"
                    hora_str = "--:--:--"
                else:
                    dt_ultima = datetime.strptime(f"{ahora_dt.strftime('%Y-%m-%d')} {hora_str}", "%Y-%m-%d %H:%M:%S")
                    diferencia_seg = (ahora_dt - dt_ultima).total_seconds()

                    if diferencia_seg > 3600:
                        alerta = True
                        mensaje_estado = "SIN TRANSACCIONES ÚLTIMA HORA"

                resultado_clientes.append({
                    "cod_cliente": str(cli["cod_cliente"]),
                    "descripcion": str(nombre_cli),
                    "tabla": tabla_cli,
                    "ultima_hora": hora_str,
                    "tx_hoy": 1 if hora_str != "--:--:--" else 0,
                    "alerta": alerta,
                    "mensaje": mensaje_estado
                })

            except Exception:
                resultado_clientes.append({
                    "cod_cliente": str(cli["cod_cliente"]),
                    "descripcion": str(nombre_cli),
                    "tabla": tabla_cli,
                    "ultima_hora": "--:--:--",
                    "tx_hoy": 0,
                    "alerta": True,
                    "mensaje": "SIN TABLA / INACTIVO"
                })

        conn.close()

    except Exception as e:
        print(f"Error consultando monitoreo de clientes: {e}")

    return resultado_clientes