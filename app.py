from contextlib import asynccontextmanager
import threading
import time
from datetime import datetime, timedelta
from typing import List
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import pymysql

DB_HOST = "199.14.10.150"
DB_USER = "datawuser"
DB_PASS = "datawpassword"
DB_NAME = "Dataw"

ENTIDADES_MAP = {
    "0304": "LUKA", "1005": "BCP", "1017": "BANCO SOL",
    "0035": "BANCO GANADERO", "0009": "BMSC", "0008": "BNB",
    "0004": "BISA", "5006": "ECOFUTURO", "9065": "DIACONIA"
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

@app.get("/api/v1/monitoreo-clientes")
def get_monitoreo_clientes():
    resultado_clientes = []
    
    TABLAS_EXCLUIDAS = ["mov_rentdig", "mov_sip", "mov_unibienes", "mov_bja"]

    try:
        conn = pymysql.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
            connect_timeout=5, read_timeout=10, cursorclass=pymysql.cursors.DictCursor
        )
        cursor = conn.cursor()

        cursor.execute("SELECT NOW() AS ahora")
        db_now = cursor.fetchone()["ahora"]
        ahora_dt = db_now if isinstance(db_now, datetime) else datetime.now()

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
                    "color_estado": "VERDE",
                    "mensaje": "OMITIDO POR TAMAÑO"
                })
                continue

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

                if not hora_str:
                    color_estado = "ROJO"
                    mensaje_estado = "SIN TRANSACCIONES HOY"
                    hora_str = "--:--:--"
                else:
                    dt_ultima = datetime.strptime(f"{ahora_dt.strftime('%Y-%m-%d')} {hora_str}", "%Y-%m-%d %H:%M:%S")
                    diferencia_seg = (ahora_dt - dt_ultima).total_seconds()

                    if diferencia_seg <= 3600:
                        color_estado = "VERDE"
                        mensaje_estado = "OK"
                    else:
                        color_estado = "NARANJA"
                        mensaje_estado = "SIN TX ÚLTIMA HORA"

                resultado_clientes.append({
                    "cod_cliente": str(cli["cod_cliente"]),
                    "descripcion": str(nombre_cli),
                    "tabla": tabla_cli,
                    "ultima_hora": hora_str,
                    "tx_hoy": 1 if hora_str != "--:--:--" else 0,
                    "color_estado": color_estado,
                    "mensaje": mensaje_estado
                })

            except Exception:
                resultado_clientes.append({
                    "cod_cliente": str(cli["cod_cliente"]),
                    "descripcion": str(nombre_cli),
                    "tabla": tabla_cli,
                    "ultima_hora": "--:--:--",
                    "tx_hoy": 0,
                    "color_estado": "ROJO",
                    "mensaje": "SIN TABLA / INACTIVO"
                })

        conn.close()

    except Exception as e:
        print(f"Error consultando monitoreo de clientes: {e}")

    return resultado_clientes

@app.get("/api/v1/canales")
def obtener_lista_canales():
    canales = []
    try:
        conn = pymysql.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
            connect_timeout=5, read_timeout=10, cursorclass=pymysql.cursors.DictCursor
        )
        cursor = conn.cursor()
        cursor.execute("SELECT cod_entidad, descripcion FROM entidad ORDER BY descripcion ASC")
        rows = cursor.fetchall()
        for r in rows:
            canales.append({
                "cod_entidad": str(r["cod_entidad"]),
                "descripcion": str(r["descripcion"])
            })
        conn.close()
    except Exception as e:
        print(f"Error consultando catalogo de entidades: {e}")
    return canales

def clasificar_origen_sql(col_origen="origen"):
    return f"""
        CASE 
            WHEN UPPER(TRIM(COALESCE({col_origen}, ''))) IN ('APWS', 'APMO', 'APWY', 'APWB', 'APWA') THEN 'Banca Digital'
            WHEN UPPER(TRIM(COALESCE({col_origen}, ''))) = 'LUKA' THEN 'Whatsapp'
            WHEN UPPER(TRIM(COALESCE({col_origen}, ''))) = 'APME' THEN 'Mediador'
            
            -- Categoría Físico (Ampliación de orígenes y comodines)
            WHEN UPPER(TRIM(COALESCE({col_origen}, ''))) IN ('APTE', 'APWE') THEN 'Físico'
            WHEN UPPER(TRIM(COALESCE({col_origen}, ''))) IN ('GSEF', 'GSIS', 'GSQN', 'GSTC') THEN 'GenesisWeb'
            WHEN UPPER(TRIM(COALESCE({col_origen}, ''))) IN ('APMP', 'MREF') THEN 'Multired'
            WHEN UPPER(TRIM(COALESCE({col_origen}, ''))) LIKE 'MC%' OR UPPER(TRIM(COALESCE({col_origen}, ''))) LIKE 'MW%' 
                 OR UPPER(TRIM(COALESCE({col_origen}, ''))) LIKE 'ME%' OR UPPER(TRIM(COALESCE({col_origen}, ''))) LIKE 'MR%' THEN 'Mis Cuentas'
            WHEN UPPER(TRIM(COALESCE({col_origen}, ''))) LIKE 'SP%' OR UPPER(TRIM(COALESCE({col_origen}, ''))) LIKE 'MS%' THEN 'Suite de Pagos'
            WHEN UPPER(TRIM(COALESCE({col_origen}, ''))) IN ('APEF', 'APIF', 'APTF', 'POS', 'CAJA', 'VENTANILLA') THEN 'Otros'
            ELSE 'Otros'
        END
    """

@app.get("/api/v1/monitoreo-canal")
def get_monitoreo_canal(
    cod_entidad: str = Query(..., description="Código de entidad/banco"),
    fecha_inicio: str = Query(..., description="YYYY-MM-DD"),
    fecha_fin: str = Query(..., description="YYYY-MM-DD"),
    hora_inicio: str = Query("00:00", description="HH:MM"),
    hora_fin: str = Query("23:59", description="HH:MM")
):
    CATEGORIAS = [
        'Banca Digital', 'Whatsapp', 'Mediador', 'Físico',
        'GenesisWeb', 'Multired', 'Mis Cuentas', 'Suite de Pagos', 'Otros'
    ]

    try:
        dt_ini = datetime.strptime(fecha_inicio, "%Y-%m-%d")
        dt_fin = datetime.strptime(fecha_fin, "%Y-%m-%d")
        if (dt_fin - dt_ini).days > 14:
            dt_fin = dt_ini + timedelta(days=14)
            fecha_fin = dt_fin.strftime("%Y-%m-%d")
    except Exception:
        pass

    TABLAS_EXCLUIDAS = ["mov_rentdig", "mov_sip", "mov_unibienes", "mov_bja"]

    marcas_tiempo = []
    curr_dt = dt_ini
    h_ini_int = int(hora_inicio.split(":")[0])
    h_fin_int = int(hora_fin.split(":")[0])

    while curr_dt <= dt_fin:
        for h in range(0, 24):
            if h_ini_int <= h <= h_fin_int:
                marcas_tiempo.append(f"{curr_dt.strftime('%Y-%m-%d')} {h:02d}:00")
        curr_dt += timedelta(days=1)

    matriz_datos = {m: {cat: 0 for cat in CATEGORIAS} for m in marcas_tiempo}
    
    # 📌 ESTRUCTURA DE DESGROSE DE CLIENTES POR MARCA DE TIEMPO Y TIPO DE ORIGEN
    desglose_clientes = {m: {cat: {} for cat in CATEGORIAS} for m in marcas_tiempo}

    try:
        conn = pymysql.connect(
            host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
            connect_timeout=5, read_timeout=15, cursorclass=pymysql.cursors.DictCursor
        )
        cursor = conn.cursor()

        cursor.execute("SELECT cod_cliente, descripcion, tabla FROM cliente WHERE UPPER(TRIM(estado)) = 'V'")
        tablas_rows = cursor.fetchall()

        case_sql = clasificar_origen_sql("origen")

        for row_t in tablas_rows:
            t_raw = str(row_t.get("tabla") or "").strip().lower()
            if not t_raw: continue
            tabla_name = t_raw if t_raw.startswith("mov_") else f"mov_{t_raw}"
            
            nombre_cliente = str(row_t.get("descripcion") or row_t.get("cod_cliente") or "Cliente").strip()

            if tabla_name in TABLAS_EXCLUIDAS: continue

            query = f"""
                SELECT 
                    DATE_FORMAT(ADDTIME(CAST(fecha AS DATETIME), hora), '%Y-%m-%d %H:00') AS fecha_hora,
                    {case_sql} AS grupo_origen,
                    COUNT(*) AS total
                FROM {tabla_name}
                WHERE fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
                  AND hora BETWEEN '{hora_inicio}:00' AND '{hora_fin}:59'
                  AND UPPER(TRIM(estado)) = 'P'
                  AND CAST(cod_entidad AS CHAR) = '{cod_entidad}'
                GROUP BY fecha_hora, grupo_origen;
            """
            try:
                cursor.execute(query)
                res = cursor.fetchall()
                for r in res:
                    fh = r["fecha_hora"]
                    grp = r["grupo_origen"]
                    tot = int(r["total"])

                    if fh in matriz_datos and grp in matriz_datos[fh]:
                        matriz_datos[fh][grp] += tot
                        
                        # Guardar desglose por cliente
                        if nombre_cliente not in desglose_clientes[fh][grp]:
                            desglose_clientes[fh][grp][nombre_cliente] = 0
                        desglose_clientes[fh][grp][nombre_cliente] += tot

            except Exception as ex_t:
                continue

        conn.close()

    except Exception as e:
        print(f"Error consultando monitoreo canal: {e}")

    series = {cat: [] for cat in CATEGORIAS}
    for m in marcas_tiempo:
        for cat in CATEGORIAS:
            series[cat].append(matriz_datos[m][cat])

    return {
        "marcas_tiempo": marcas_tiempo,
        "series": series,
        "desglose_clientes": desglose_clientes
    }