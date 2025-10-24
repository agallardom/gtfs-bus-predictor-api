import pandas as pd
import datetime
import pytz
import sys
import math # Necesario para la función haversine
import os   # Necesario para leer variables de entorno (la URL remota)
import json # Necesario para procesar el JSON
import requests # Necesario para descargar el JSON remoto
from flask import Flask, jsonify, request # Necesario para crear la API
from flask_cors import CORS 


# --- CONFIGURACIÓN ---
RUTA_GTFS = './gtfs_data/'
ZONA_HORARIA = 'Europe/Madrid' 
HORA_FORMATO = "%H:%M"

# 🛑 AÑADE ESTE DICCIONARIO GLOBAL
# Define las coordenadas de referencia para cada grupo de paradas.
# SUSTITUYE con las coordenadas reales de los puntos centrales de tus grupos.
GRUPO_COORDENADAS = {
    "CASA": (41.53904, 2.11787),     # Ejemplo: Latitud y Longitud de tu casa
    "CASA_MARIA": (41.57131, 2.08258)  # Ejemplo: Latitud y Longitud de tu trabajo
}

# 🛑 NUEVO: DICCIONARIO DE GRUPOS DE PARADAS
GRUPOS_PARADAS = {
    # Puedes definir tantos grupos como quieras
    "CASA": [
        'TUS_14165',  # Sant Oleguer 
        'TUS_14166',  # Bellesguard
        'TUS_14309',  # Eixample
        'ROD_78704',  # Sabadell Centre
        'FGC_PJ'  # Plaça Major
    ],
    "CASA_MARIA": [
        'TUS_14360',  # Àger
        'TUS_14227',  # La Roureda 1
        'TUS_16590',  # La Roureda 2
        'TUS_14300',  # Sant Julià
        'TUS_14398',  # El Farell
        'ROD_78709',  # Sabadell Nord
        'FGC_PN'  # Parc del Nord
    ],
    "TREN": [
        'ROD_78704',  # Estación RENFE
        'FGC_PJ'      # Estación FGC
    ]
}

# 🛑 La clave 'DEFAULT' es el grupo que se usará si no se especifica ninguno
GRUPO_DEFAULT = "CASA"

# =======================================================================
# CONFIGURACIÓN DE URL REMOTA
# =======================================================================

# La API lee la URL remota de una Variable de Entorno de Render.
REMOTE_CONFIG_URL = os.environ.get(
    "USER_GROUPS_JSON_URL", 
    "https://angelgallardo.com.es/bus_predictor/config.json" # URL por defecto si no está en Render
)

# =======================================================================
# FUNCIONES DE UTILIDAD
# =======================================================================

# Nueva función para calcular la distancia (Fórmula Haversine para geolocalización)
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Radio de la Tierra en kilómetros
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance = R * c
    return distance

def fetch_remote_user_groups():
    """Descarga el JSON de configuración desde la URL remota."""
    try:
        response = requests.get(REMOTE_CONFIG_URL, timeout=10)
        response.raise_for_status() # Lanza un error HTTP si la descarga falla
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ ERROR al descargar la configuración remota desde {REMOTE_CONFIG_URL}: {e}")
        return None 
    except Exception as e:
        print(f"❌ ERROR inesperado al procesar JSON remoto: {e}")
        return None

app = Flask(__name__)
CORS(app)  # 2. Habilitar CORS para TODA la aplicación

# =======================================================
# 3. Carga GLOBAL DE DATOS (fuera de las rutas)
# =======================================================
# Aquí iría la carga de datos si estuviera fuera de main_predictor()
# ...

# ===================================================
# FUNCIONES AUXILIARES (SE MANTIENEN IGUAL)
# ===================================================

def obtener_lineas_id_parada(parada_id, df_horarios_base, routes_df):
    """
    Identifica y lista todos los IDs, nombres cortos y destinos de las líneas que pasan.
    """
    
    # 1. Filtrar solo los horarios de la parada actual
    df_parada = df_horarios_base[df_horarios_base['stop_id'] == parada_id]
    
    # 2. Agrupar por route_id y trip_headsign para obtener las combinaciones únicas de línea y destino
    # ESTO ES LO CRUCIAL: Agrupamos por destino para separar las direcciones (ej. R4 Martorell vs R4 Manresa)
    rutas_por_destino = df_parada.groupby(['route_id', 'trip_headsign'])['trip_id'].count().reset_index()
    
    # 3. Unir con routes para obtener el nombre corto de la línea
    rutas_con_nombre = pd.merge(
        rutas_por_destino[['route_id', 'trip_headsign']], 
        routes_df, 
        on='route_id', 
        how='left'
    )
    
    # 4. Formatear el resultado como una lista de tuplas para iterar: 
    # [(route_id, route_short_name, trip_headsign), ...]
    resultados = []
    for index, row in rutas_con_nombre.iterrows():
        resultados.append((row['route_id'], row['route_short_name'], row['trip_headsign']))
        
    # Devuelve la lista de tuplas única
    return resultados

# ----------------------------------------------------
# SEGUNDA FUNCION AUXILIAR (CALCULADORA DE BUSES)
# ----------------------------------------------------
def calcular_proximos_buses(parada_id, nombre_parada, df_horarios_base, routes_df, ahora, tiempo_actual_str):
    """Calcula los próximos horarios para una única parada, línea por línea."""
    
    # 1. Obtener las combinaciones únicas de (ID de línea, Nombre corto, Destino)
    lineas_con_destino = obtener_lineas_id_parada(parada_id, df_horarios_base, routes_df) 

    # 2. Filtrar horarios base para esta parada
    df_horarios_parada = df_horarios_base[df_horarios_base['stop_id'] == parada_id]
    
    resultados_por_linea = []

    for route_id, route_short_name, trip_headsign in lineas_con_destino: 
        # 3. Filtrar los horarios SOLAMENTE para esta línea Y ESTE DESTINO
        # Añadimos el filtro por trip_headsign
        df_linea = df_horarios_parada[
            (df_horarios_parada['route_id'] == route_id) & 
            (df_horarios_parada['trip_headsign'] == trip_headsign)
        ]
        
        # Horarios después de la hora actual
        proximos_horarios = df_linea[df_linea['departure_time'] > tiempo_actual_str]
        
        # Ordenar por hora y tomar los dos primeros
        proximos_horarios = proximos_horarios.sort_values(by='departure_time').head(2)

        # 4. Capturar los resultados
        resultado_linea = {
            'linea': route_short_name,
            'proximo_bus': 'N/A',
            'siguiente_bus': 'N/A',
            'destino': 'N/A',
            'minutos_restantes': 'N/A'
        }

        if not proximos_horarios.empty:
            # Próximo Bus
            proximo_hora_str = proximos_horarios['departure_time'].iloc[0][:5] 
            proximo_destino = proximos_horarios['trip_headsign'].iloc[0] 
            
            # Calcular tiempo restante (código que ya tenías)
            try:
                hora_salida = datetime.datetime.strptime(proximo_hora_str, '%H:%M').time()
            except ValueError:
                # Si hay un error de formato GTFS (ej. 25:XX:XX), saltar.
                continue 

            dt_proximo = ahora.replace(hour=hora_salida.hour, minute=hora_salida.minute, second=0, microsecond=0)
            if dt_proximo < ahora:
                dt_proximo += datetime.timedelta(days=1)
            delta = dt_proximo - ahora
            minutos_restantes = int(delta.total_seconds() // 60)
            
            # Siguiente Bus
            siguiente_hora_str = "N/A"
            if len(proximos_horarios) > 1:
                siguiente_hora_str = proximos_horarios['departure_time'].iloc[1][:5]

            # Actualizar el diccionario de resultado
            resultado_linea.update({
                'proximo_bus': proximo_hora_str,
                'siguiente_bus': siguiente_hora_str,
                # Usamos el destino de la iteración
                'destino': trip_headsign, 
                'minutos_restantes': minutos_restantes
            })
        
        resultados_por_linea.append(resultado_linea)
    
    return {'nombre_parada': nombre_parada, 'horarios': resultados_por_linea}


# ===================================================
# FUNCIÓN PRINCIPAL DE EJECUCIÓN (main_predictor)
# ===================================================

def main_predictor(group_name=GRUPO_DEFAULT): 
    """Carga todos los datos GTFS y procesa todas las paradas definidas."""
    
    try:
        # 1. Cargar datos necesarios
        stops = pd.read_csv(RUTA_GTFS + 'stops.txt', usecols=['stop_id', 'stop_name'])
        stop_times = pd.read_csv(RUTA_GTFS + 'stop_times.txt', usecols=['trip_id', 'departure_time', 'stop_id'])
        trips = pd.read_csv(RUTA_GTFS + 'trips.txt', usecols=['trip_id', 'service_id', 'trip_headsign', 'route_id'])
        calendar = pd.read_csv(RUTA_GTFS + 'calendar.txt')
        calendar_dates = pd.read_csv(RUTA_GTFS + 'calendar_dates.txt')
        routes = pd.read_csv(RUTA_GTFS + 'routes.txt', usecols=['route_id', 'route_short_name', 'route_long_name'])
        
    except FileNotFoundError as e:
        # Esto será capturado por la ruta de Flask
        raise Exception(f"Error de Archivo: No se encontró un archivo GTFS. {e}")
    except Exception as e:
        raise Exception(f"Error al cargar datos GTFS: {e}")


    # 2. Definir la hora actual y servicio
    tz = pytz.timezone(ZONA_HORARIA)
    ahora = datetime.datetime.now(tz)
    tiempo_actual_str = ahora.strftime('%H:%M:%S') 
    fecha_hoy_gtfs = int(ahora.strftime('%Y%m%d'))
    
    # 3. Lógica de servicio activo (combinando calendar y calendar_dates)
    dias_semana = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    dia_hoy_columna = dias_semana[ahora.weekday()]
    
    servicios_base = calendar[calendar[dia_hoy_columna] == 1]['service_id'].tolist()
    servicios_añadidos = calendar_dates[(calendar_dates['date'] == fecha_hoy_gtfs) & (calendar_dates['exception_type'] == 1)]['service_id'].tolist()
    servicios_cancelados = calendar_dates[(calendar_dates['date'] == fecha_hoy_gtfs) & (calendar_dates['exception_type'] == 2)]['service_id'].tolist()
    
    servicios_activos = set(servicios_base)
    servicios_activos.update(servicios_añadidos)
    servicios_activos.difference_update(servicios_cancelados)
    
    if not servicios_activos:
        return "No hay servicios programados para hoy."

    trips_hoy = trips[trips['service_id'].isin(servicios_activos)]
    df_horarios_base = pd.merge(stop_times, trips_hoy, on='trip_id', how='inner')

    # 4. Obtener la lista de paradas del grupo solicitado
    
    # Verificar si el grupo existe en la configuración
    if group_name not in GRUPOS_PARADAS:
        grupos_disponibles = ", ".join(GRUPOS_PARADAS.keys())
        raise ValueError(f"El grupo '{group_name}' no existe. Grupos disponibles: {grupos_disponibles}")
        
    # Obtener la lista de IDs a procesar
    paradas_a_procesar = GRUPOS_PARADAS[group_name]
    
    # 5. Iniciar el procesamiento de múltiples paradas
    resultados_totales = {}
    
    for parada_id in paradas_a_procesar:
        
        # Obtener el nombre de la parada
        try:
            nombre_parada = stops.loc[stops['stop_id'] == parada_id, 'stop_name'].iloc[0]
        except IndexError:
            resultados_totales[parada_id] = {'error': f"ID {parada_id} no encontrado en stops.txt."}
            continue
            
        # Llamar a la función de cálculo por parada
        resultados_parada = calcular_proximos_buses(
            parada_id, 
            nombre_parada,
            df_horarios_base,
            routes, 
            ahora, 
            tiempo_actual_str
        )
        
        # -----------------------------------------------------------------
        # 🛑 NUEVA LÓGICA DE ORDENAMIENTO POR TIEMPO (MINUTOS RESTANTES)
        # -----------------------------------------------------------------
        
        # 5a. Filtrar solo buses que tienen un horario válido (no 'N/A')
        horarios_validos = [
            res for res in resultados_parada['horarios'] 
            if res['minutos_restantes'] != 'N/A'
        ]
        
        # 5b. Ordenar por el campo 'minutos_restantes' (el bus que llega antes)
        horarios_ordenados = sorted(
            horarios_validos, 
            key=lambda x: x['minutos_restantes']
        )
        
        # 5c. Almacenar la lista ordenada en el resultado
        resultados_parada['horarios_ordenados'] = horarios_ordenados
        resultados_totales[parada_id] = resultados_parada
        
    # 6. Devolver el diccionario completo
    return resultados_totales
            

# ===================================================
# RUTA WEB PARA SERVIR LA API (LA FUNCIÓN QUE DEBE LLEVAR EL DECORADOR)
# ===================================================

# 🛑 NUEVA RUTA: Determinar el grupo más cercano 🛑
@app.route('/api/nearest', methods=['GET'])
def get_nearest_group():
    # 1. Obtener coordenadas del usuario desde la URL
    user_lat = request.args.get('lat', type=float)
    user_lon = request.args.get('lon', type=float)

    if user_lat is None or user_lon is None:
        return jsonify({"error": "Faltan parámetros 'lat' o 'lon'"}), 400

    min_distance = float('inf')
    nearest_group_name = None

    # 2. Iterar sobre todos los grupos y calcular la distancia
    for group_name, (group_lat, group_lon) in GRUPO_COORDENADAS.items():
        distance = haversine(user_lat, user_lon, group_lat, group_lon)

        if distance < min_distance:
            min_distance = distance
            nearest_group_name = group_name

    # 3. Devolver el nombre del grupo más cercano
    return jsonify({"nearest_group": nearest_group_name, "distance_km": round(min_distance, 2)})

@app.route('/api/bus/<string:grupo>', methods=['GET'])
def get_bus_schedule(grupo):
    """Ruta que calcula y devuelve los horarios de un grupo de paradas en JSON."""
    
    try:
        # Llamar al predictor con el grupo seleccionado
        resultados = main_predictor(group_name=grupo)
        
        # Si main_predictor devuelve un string (ej. "No hay servicios..."), manejarlo como error
        if isinstance(resultados, str):
             return jsonify({"error": resultados}), 500
        
        # Devuelve el diccionario de resultados en formato JSON
        return jsonify(resultados)

    except ValueError as e:
        # Captura el error si el grupo no existe (lanzado desde main_predictor)
        return jsonify({"error": str(e)}), 404
        
    except Exception as e:
        # Captura errores de archivo u otros errores de procesamiento
        return jsonify({"error": f"Error interno del servidor: {str(e)}"}), 500

# ===================================================
# INICIO DE LA APLICACIÓN
# ===================================================

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
