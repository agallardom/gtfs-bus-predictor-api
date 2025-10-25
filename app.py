import pandas as pd
import datetime
import pytz
import math
import os
import json
import requests
from flask import Flask, jsonify, request
from flask_cors import CORS 

# =======================================================================
# CONFIGURACIÓN Y CONSTANTES
# =======================================================================

RUTA_GTFS = './gtfs_data/'
ZONA_HORARIA = 'Europe/Madrid' 
HORA_FORMATO = "%H:%M"

# La API lee la URL remota de una Variable de Entorno de Render.
# ¡Asegúrate de que esta URL esté configurada en Render!
REMOTE_CONFIG_URL = os.environ.get(
    "USER_GROUPS_JSON_URL", 
    "https://angelgallardo.com.es/bus_predictor/config.json" # URL por defecto
)

app = Flask(__name__)

# =================================================================
# CORRECCIÓN CRÍTICA: CONFIGURACIÓN CORS EXPLÍCITA
# Esto soluciona el error 'Access-Control-Allow-Origin'.
# Permitimos todos los orígenes para la comunicación entre dominios.
# =================================================================
CORS(app, resources={r"/*": {"origins": "*"}}) 
# =================================================================


# Variables globales para almacenar los datos GTFS cargados una sola vez
# Esto evita recargar los archivos .txt en cada petición.
GTFS_DATA = None 

# =======================================================================
# FUNCIONES DE UTILIDAD PARA CONFIGURACIÓN REMOTA
# =======================================================================

def haversine(lat1, lon1, lat2, lon2):
    """Calcula la distancia Haversine (en kilómetros) entre dos puntos GPS."""
    R = 6371  # Radio de la Tierra en kilómetros
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance = R * c
    return distance

def _load_remote_config(url):
    """Carga la configuración de usuario y grupos desde la URL remota."""
    
    print(f"Descargando configuración remota de: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() 
        config_data = response.json()
        
        print("Configuración remota cargada exitosamente.")
        return config_data

    except requests.exceptions.RequestException as e:
        print(f"ERROR: No se pudo cargar la configuración remota. {e}")
        raise ConnectionError(f"ERROR CRÍTICO: No se pudo acceder a la configuración remota. Verificar URL o conexión.")

def _get_user_config(key):
    """
    Obtiene la configuración específica para la clave de usuario proporcionada.
    """
    try:
        # Aquí se llama a la función sin caché
        config = _load_remote_config(REMOTE_CONFIG_URL) 
        if key not in config:
            raise KeyError(f"Clave de usuario '{key}' no encontrada en el JSON remoto.")
        return config[key]
    except (ConnectionError, KeyError) as e:
        # Re-lanza la excepción para que el decorador la capture y devuelva 400 o 500
        raise e
    except Exception as e:
        raise Exception(f"Error inesperado al procesar la configuración: {e}")

# =======================================================================
# 🛑 NUEVA FUNCIÓN: CARGA ÚNICA DE DATOS GTFS 🛑
# =======================================================================

def load_gtfs_data():
    """Carga y pre-procesa los archivos GTFS. Se llama al inicio de la aplicación."""
    global GTFS_DATA
    if GTFS_DATA is not None:
        return GTFS_DATA

    print("Cargando y pre-procesando datos GTFS...")
    try:
        stops_df = pd.read_csv(RUTA_GTFS + 'stops.txt', usecols=['stop_id', 'stop_name', 'stop_lat', 'stop_lon']) # Añadida lat/lon para la ruta /api/nearest
        stop_times_df = pd.read_csv(RUTA_GTFS + 'stop_times.txt', usecols=['trip_id', 'departure_time', 'stop_id'])
        trips_df = pd.read_csv(RUTA_GTFS + 'trips.txt', usecols=['trip_id', 'service_id', 'trip_headsign', 'route_id'])
        calendar_df = pd.read_csv(RUTA_GTFS + 'calendar.txt')
        calendar_dates_df = pd.read_csv(RUTA_GTFS + 'calendar_dates.txt')
        routes_df = pd.read_csv(RUTA_GTFS + 'routes.txt', usecols=['route_id', 'route_short_name', 'route_long_name'])
        
    except FileNotFoundError as e:
        print(f"ERROR: No se encontró un archivo GTFS: {e}")
        # En producción, esto debería abortar el servicio
        return None 
    
    GTFS_DATA = {
        'stops': stops_df,
        'stop_times': stop_times_df,
        'trips': trips_df,
        'calendar': calendar_df,
        'calendar_dates': calendar_dates_df,
        'routes': routes_df
    }
    print("Carga GTFS completada.")
    return GTFS_DATA

# =======================================================================
# LÓGICA EXISTENTE DE CÁLCULO DE HORARIOS (REFACTORIZADA)
# =======================================================================

# Mantenemos las funciones auxiliares exactamente como estaban
def obtener_lineas_id_parada(parada_id, df_horarios_base, routes_df):
    """Identifica y lista todos los IDs, nombres cortos y destinos de las líneas que pasan."""
    # ... Tu lógica se mantiene intacta ...
    df_parada = df_horarios_base[df_horarios_base['stop_id'] == parada_id]
    rutas_por_destino = df_parada.groupby(['route_id', 'trip_headsign'])['trip_id'].count().reset_index()
    rutas_con_nombre = pd.merge(
        rutas_por_destino[['route_id', 'trip_headsign']], 
        routes_df, 
        on='route_id', 
        how='left'
    )
    resultados = []
    for index, row in rutas_con_nombre.iterrows():
        resultados.append((row['route_id'], row['route_short_name'], row['trip_headsign']))
    return resultados


def calcular_proximos_buses(parada_id, nombre_parada, df_horarios_base, routes_df, ahora, tiempo_actual_str):
    """Calcula los próximos horarios para una única parada, línea por línea."""
    # ... Tu lógica se mantiene intacta ...
    lineas_con_destino = obtener_lineas_id_parada(parada_id, df_horarios_base, routes_df) 
    df_horarios_parada = df_horarios_base[df_horarios_base['stop_id'] == parada_id]
    resultados_por_linea = []

    for route_id, route_short_name, trip_headsign in lineas_con_destino: 
        df_linea = df_horarios_parada[
            (df_horarios_parada['route_id'] == route_id) & 
            (df_horarios_parada['trip_headsign'] == trip_headsign)
        ]
        
        proximos_horarios = df_linea[df_linea['departure_time'] > tiempo_actual_str]
        proximos_horarios = proximos_horarios.sort_values(by='departure_time').head(2)

        resultado_linea = {
            'linea': route_short_name,
            'proximo_bus': 'N/A',
            'siguiente_bus': 'N/A',
            'destino': 'N/A',
            'minutos_restantes': 'N/A'
        }

        if not proximos_horarios.empty:
            proximo_hora_str = proximos_horarios['departure_time'].iloc[0][:5] 
            proximo_destino = proximos_horarios['trip_headsign'].iloc[0] 
            
            try:
                hora_salida = datetime.datetime.strptime(proximo_hora_str, '%H:%M').time()
            except ValueError:
                # Esto maneja el formato GTFS donde la hora puede ser > 23:59
                # Aquí podrías necesitar lógica más robusta si se usa hora > 24
                continue 

            dt_proximo = ahora.replace(hour=hora_salida.hour, minute=hora_salida.minute, second=0, microsecond=0)
            if dt_proximo < ahora:
                dt_proximo += datetime.timedelta(days=1)
            delta = dt_proximo - ahora
            minutos_restantes = int(delta.total_seconds() // 60)
            
            siguiente_hora_str = "N/A"
            if len(proximos_horarios) > 1:
                siguiente_hora_str = proximos_horarios['departure_time'].iloc[1][:5]

            resultado_linea.update({
                'proximo_bus': proximo_hora_str,
                'siguiente_bus': siguiente_hora_str,
                'destino': trip_headsign, 
                'minutos_restantes': minutos_restantes
            })
        
        resultados_por_linea.append(resultado_linea)
    
    return {'nombre_parada': nombre_parada, 'horarios': resultados_por_linea}


def process_schedules_for_stops(paradas_a_procesar, gtfs_data):
    """
    Función que sustituye la lógica central de main_predictor.
    Procesa los horarios para la lista de IDs de parada proporcionada.
    """
    stops_df = gtfs_data['stops']
    routes_df = gtfs_data['routes']
    trips_df = gtfs_data['trips']
    stop_times_df = gtfs_data['stop_times']
    calendar_df = gtfs_data['calendar']
    calendar_dates_df = gtfs_data['calendar_dates']

    # 1. Definir la hora actual y servicio
    tz = pytz.timezone(ZONA_HORARIA)
    ahora = datetime.datetime.now(tz)
    tiempo_actual_str = ahora.strftime('%H:%M:%S') 
    fecha_hoy_gtfs = int(ahora.strftime('%Y%m%d'))
    
    # 2. Lógica de servicio activo
    dias_semana = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    dia_hoy_columna = dias_semana[ahora.weekday()]
    
    servicios_base = calendar_df[calendar_df[dia_hoy_columna] == 1]['service_id'].tolist()
    servicios_añadidos = calendar_dates_df[(calendar_dates_df['date'] == fecha_hoy_gtfs) & (calendar_dates_df['exception_type'] == 1)]['service_id'].tolist()
    servicios_cancelados = calendar_dates_df[(calendar_dates_df['date'] == fecha_hoy_gtfs) & (calendar_dates_df['exception_type'] == 2)]['service_id'].tolist()
    
    servicios_activos = set(servicios_base)
    servicios_activos.update(servicios_añadidos)
    servicios_activos.difference_update(servicios_cancelados)
    
    if not servicios_activos:
        return "No hay servicios programados para hoy."

    trips_hoy = trips_df[trips_df['service_id'].isin(servicios_activos)]
    df_horarios_base = pd.merge(stop_times_df, trips_hoy, on='trip_id', how='inner')
    
    # 3. Iniciar el procesamiento de múltiples paradas
    resultados_totales = {}
    
    for parada_id in paradas_a_procesar:
        
        try:
            nombre_parada = stops_df.loc[stops_df['stop_id'] == parada_id, 'stop_name'].iloc[0]
        except IndexError:
            resultados_totales[parada_id] = {'error': f"ID {parada_id} no encontrado en stops.txt."}
            continue
            
        resultados_parada = calcular_proximos_buses(
            parada_id, 
            nombre_parada,
            df_horarios_base,
            routes_df, 
            ahora, 
            tiempo_actual_str
        )
        
        # Lógica de ordenamiento por tiempo (se mantiene)
        horarios_validos = [
            res for res in resultados_parada['horarios'] 
            if res['minutos_restantes'] != 'N/A'
        ]
        horarios_ordenados = sorted(
            horarios_validos, 
            key=lambda x: x['minutos_restantes']
        )
        resultados_parada['horarios_ordenados'] = horarios_ordenados
        resultados_totales[parada_id] = resultados_parada
        
    return resultados_totales


# =======================================================================
# RUTAS DE LA API (MODIFICADAS PARA USAR 'user_key')
# =======================================================================
initial_setup_done = False

@app.before_request
def run_once_setup():
    """Ejecuta código de configuración solo una vez."""
    global initial_setup_done
    
    if not initial_setup_done:
        print("Ejecutando configuración inicial (Carga GTFS)...")
        # Llama a la función que REALMENTE quieres ejecutar una vez:
        load_gtfs_data() 
        
        initial_setup_done = True

# Endpoint: /api/config
@app.route('/api/config', methods=['GET'])
def get_config(user_config):
    """
    Devuelve la configuración completa del usuario (grupos y paradas).
    """
    return jsonify(user_config)

@app.route('/api/nearest', methods=['GET'])
def get_nearest_group():
    """Ruta para determinar el grupo más cercano, usando la configuración del usuario."""
    user_key = request.args.get('key') 
    user_lat = request.args.get('lat', type=float)
    user_lon = request.args.get('lon', type=float)

    if not user_key or user_lat is None or user_lon is None:
        return jsonify({"error": "Faltan parámetros 'key', 'lat' o 'lon'."}), 400

    user_groups_db = _load_remote_config(REMOTE_CONFIG_URL)
    if user_groups_db is None:
        return jsonify({"error": "No se pudo cargar la base de datos de grupos remota."}), 500

    user_config = user_groups_db.get(user_key)
    if not user_config:
        return jsonify({"error": f"Clave de usuario '{user_key}' no encontrada en el JSON remoto."}), 404

    min_distance = float('inf')
    nearest_group_name = None

    # Iterar sobre la configuración anidada del usuario
    for group_name, config_data in user_config.items():
        try:
            # Extraer las coordenadas del diccionario anidado
            coords_str = config_data.get('coords')
            if not coords_str: continue 
            
            group_lat, group_lon = map(float, coords_str.split(',')) 
            
            distance = haversine(user_lat, user_lon, group_lat, group_lon)

            if distance < min_distance:
                min_distance = distance
                nearest_group_name = group_name
        
        except (ValueError, AttributeError):
            continue 

    if nearest_group_name:
        return jsonify({"nearest_group": nearest_group_name, "distance_km": round(min_distance, 2)})
    else:
        return jsonify({"error": "No se encontraron grupos válidos para calcular la distancia."}), 500


@app.route('/api/bus/<string:group_name>', methods=['GET'])
def get_bus_schedule(group_name):
    """Ruta para obtener horarios de un grupo específico, usando la configuración del usuario."""
    
    user_key = request.args.get('key') 
    
    if not user_key:
        return jsonify({"error": "Falta el parámetro 'key' para identificar al usuario."}), 400

    if GTFS_DATA is None:
         return jsonify({"error": "Datos GTFS no cargados. Inténtalo de nuevo."}), 500

    user_groups_db = _load_remote_config(REMOTE_CONFIG_URL)
    if user_groups_db is None:
        return jsonify({"error": "No se pudo cargar la base de datos de grupos remota."}), 500
        
    user_config = user_groups_db.get(user_key)
    if not user_config:
        return jsonify({"error": f"Clave de usuario '{user_key}' no encontrada."}), 404

    # 1. Obtener la lista de paradas ('stops') del grupo específico del usuario
    group_data = user_config.get(group_name)
    if not group_data:
        return jsonify({"error": f"El grupo '{group_name}' no existe para el usuario '{user_key}'."}), 404

    paradas_a_procesar = group_data.get('stops', []) 
    
    if not paradas_a_procesar:
        return jsonify({"error": f"El grupo '{group_name}' no tiene paradas configuradas."}), 400

    # 2. Llamar a la lógica de procesamiento (sustituyendo a main_predictor)
    try:
        # Aquí se usa tu lógica refactorizada y se le pasa la lista de paradas
        resultados = process_schedules_for_stops(paradas_a_procesar, GTFS_DATA)
        
        if isinstance(resultados, str):
             return jsonify({"error": resultados}), 500
        
        return jsonify(resultados)

    except Exception as e:
        return jsonify({"error": f"Error interno durante el procesamiento de horarios: {str(e)}"}), 500

# =======================================================================
# INICIO DE LA APLICACIÓN
# =======================================================================

if __name__ == '__main__':
    # Carga de datos GTFS solo si se ejecuta localmente
    load_gtfs_data() 
    app.run(host='0.0.0.0', port=5000, debug=True)