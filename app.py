import pandas as pd
import datetime
import pytz
import sys
from flask import Flask, jsonify, request
from flask_cors import CORS 


# --- CONFIGURACIÓN ---
RUTA_GTFS = 'gtfs_data/'
ZONA_HORARIA = 'Europe/Madrid' 
HORA_FORMATO = "%H:%M"

# --- CONFIGURACIÓN ---
RUTA_GTFS = './gtfs_data/'
ZONA_HORARIA = 'Europe/Madrid' 
HORA_FORMATO = "%H:%M"

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

app = Flask(__name__)
CORS(app)  # 2. Habilitar CORS para TODA la aplicación

# =======================================================
# 3. Carga GLOBAL DE DATOS (fuera de las rutas)
# =======================================================
# ... tu código de carga de Pandas ...

# =======================================================
# 4. RUTAS DE FLASK
# =======================================================
@app.route('/api/bus/<string:group_name>')

# ===================================================
# FUNCIONES AUXILIARES
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

def calcular_proximos_buses(parada_id, nombre_parada, df_horarios_base, routes_df, ahora, tiempo_actual_str):
    """Calcula los próximos horarios para una única parada, línea por línea."""

    # 1. Obtener los IDs de las líneas que pasan por esta parada
    lineas_id = obtener_lineas_id_parada(parada_id, df_horarios_base, routes_df) 

    # 2. Filtrar horarios base para esta parada
    df_horarios_parada = df_horarios_base[df_horarios_base['stop_id'] == parada_id]
    
    resultados_por_linea = []


def calcular_proximos_buses(parada_id, nombre_parada, df_horarios_base, routes_df, ahora, tiempo_actual_str):
    # ... (código anterior) ...

    # 1. Obtener las combinaciones únicas de (ID de línea, Nombre corto, Destino)
    # Llama a la función modificada
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
                # Manejar el formato GTFS 25:XX:XX, si es necesario, 
                # aunque en la mayoría de casos Pandas lo maneja con la comparación de string
                continue # Saltar esta línea si hay un error de formato

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
                # Usamos el destino de la iteración, no el del viaje (que puede ser otro)
                'destino': trip_headsign, 
                'minutos_restantes': minutos_restantes
            })
        
        resultados_por_linea.append(resultado_linea)
    
    return {'nombre_parada': nombre_parada, 'horarios': resultados_por_linea}


# ===================================================
# FUNCIÓN PRINCIPAL DE EJECUCIÓN
# ===================================================

def main_predictor(grupo_a_consultar=GRUPO_DEFAULT):
    """Carga todos los datos GTFS y procesa todas las paradas definidas."""
    
    try:
        # 1. Cargar datos necesarios
        stops = pd.read_csv(RUTA_GTFS + 'stops.txt', usecols=['stop_id', 'stop_name'])
        #stops_df = pd.read_csv('./gtfs_data/stops.txt')
        stop_times = pd.read_csv(RUTA_GTFS + 'stop_times.txt', usecols=['trip_id', 'departure_time', 'stop_id'])
        trips = pd.read_csv(RUTA_GTFS + 'trips.txt', usecols=['trip_id', 'service_id', 'trip_headsign', 'route_id'])
        calendar = pd.read_csv(RUTA_GTFS + 'calendar.txt')
        calendar_dates = pd.read_csv(RUTA_GTFS + 'calendar_dates.txt')
        routes = pd.read_csv(RUTA_GTFS + 'routes.txt', usecols=['route_id', 'route_short_name', 'route_long_name'])
        
    except FileNotFoundError as e:
        return f"Error de Archivo: No se encontró un archivo GTFS. {e}"
    except Exception as e:
        return f"Error al cargar datos GTFS: {e}"

    # 2. Definir la hora actual y servicio
    tz = pytz.timezone(ZONA_HORARIA)
    ahora = datetime.datetime.now(tz)
    tiempo_actual_str = ahora.strftime('%H:%M:%S') 
    
    # *** PRUEBA DE MEDIANOCHE (si quieres ver todos los buses) ***
    # tiempo_actual_str = '00:00:00' 
    
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
    if grupo_a_consultar not in GRUPOS_PARADAS:
        grupos_disponibles = ", ".join(GRUPOS_PARADAS.keys())
        return f"Error: El grupo '{grupo_a_consultar}' no existe. Grupos disponibles: {grupos_disponibles}"
        
    # Obtener la lista de IDs a procesar
    paradas_a_procesar = GRUPOS_PARADAS[grupo_a_consultar]
    
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
        
        # 6. Formatear la salida final para la Terminal
        #salida = f"Resultados para {len(paradas_a_procesar)} paradas del grupo '{grupo_a_consultar}' ({ahora.strftime('%H:%M:%S')}):\n"    
        #for p_id, res in resultados_totales.items():
        #    if 'error' in res:
        #        salida += f"  🛑 Error en {p_id}: {res['error']}\n"
        #    else:
        #        salida += f"  --- Parada: {res['nombre_parada']} ({p_id}) ---\n"
                
        #        # 🛑 USAR LA LISTA ORDENADA
        #        for linea_data in res['horarios_ordenados']:
        #            salida += f"    Línea {linea_data['linea']} ({linea_data['destino']}): Próximo {linea_data['proximo_bus']} (en {linea_data['minutos_restantes']} min). Siguiente: {linea_data['siguiente_bus']}\n"
        #   return salida


        # 6. Formatear la salida final para la Terminal (ESTA SECCIÓN SE ELIMINA O COMENTA)
        # ---------------------------------------------------------------------------------
        # salida = f"Resultados para {len(paradas_a_procesar)} paradas... (COMENTAR O ELIMINAR)
        # ...
        # return salida 
        # ---------------------------------------------------------------------------------

        # 🛑 NUEVO: Devolver el diccionario completo
        return resultados_totales
            

# ===================================================
# RUTA WEB PARA SERVIR LA API
# ===================================================

@app.route('/api/bus/<grupo>', methods=['GET'])
def get_bus_schedule(grupo):
    """Ruta que calcula y devuelve los horarios de un grupo de paradas en JSON."""
    
    # Verifica si el grupo existe antes de ejecutar
    if grupo not in GRUPOS_PARADAS:
        return jsonify({"error": f"Grupo '{grupo}' no encontrado. Grupos disponibles: {list(GRUPOS_PARADAS.keys())}"}), 404

    # Llamar al predictor con el grupo seleccionado
    resultados = main_predictor(grupo_a_consultar=grupo)
    
    # La función main_predictor debe devolver el diccionario de resultados, NO el string de salida
    if isinstance(resultados, str) and resultados.startswith("Error"):
        return jsonify({"error": resultados}), 500
    
    # Devuelve el diccionario de resultados en formato JSON
    return jsonify(resultados)


# ===================================================
# INICIO DE LA APLICACIÓN
# ===================================================

if __name__ == '__main__':
    # 🛑 Cambiamos la forma de iniciar el script para usar el servidor Flask
    # host='0.0.0.0' es crucial para Docker/Render
    app.run(host='0.0.0.0', port=5000, debug=False)