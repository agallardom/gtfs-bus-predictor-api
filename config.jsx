import React, { useState, useMemo, useEffect } from 'react';
import { Settings, Lock, ChevronDown, ChevronUp, Users, MapPin, Map, Waypoints, Loader, AlertTriangle } from 'lucide-react';

// --- MOCK DE DATOS DEL ENDPOINT /config ---
// Esto simula la respuesta JSON que el endpoint '/config' devolvería 
// basándose en la user_key enviada (obtenida de localStorage).
const MOCK_API_RESPONSES = {
    "DEFAULT": { // Respuesta para user_key='DEFAULT'
        "CASA": {
            "coords": "41.53904, 2.11787",
            "stops": ["TUS_14165", "TUS_14166", "TUS_14309", "ROD_78704", "FGC_PJ"]
        },
        "CASA_MARIA": {
            "coords": "41.57131, 2.08258",
            "stops": ["TUS_14360", "TUS_14227", "TUS_16590", "TUS_14300", "TUS_14398", "ROD_78709", "FGC_PN"]
        }
    },
    "GUEST": { // Respuesta alternativa para simulación
        "TRABAJO": {
            "coords": "41.3851, 2.1734",
            "stops": ["MET_L1_U", "BUS_H10"]
        }
    }
};

// El componente principal App
const App = () => {
  const [isConfigVisible, setIsConfigVisible] = useState(false);
  const [expandedGroup, setExpandedGroup] = useState(null); 
  
  // Estados para manejar la data del API
  const [configData, setConfigData] = useState(null); 
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [userName, setUserName] = useState('Cargando...'); // Clave activa

  // --- LÓGICA DE CARGA DE CONFIGURACIÓN DESDE LOCALSTORAGE Y ENDPOINT /config ---
  useEffect(() => {
    // Inicializar localStorage con un valor si no existe (solo para simulación)
    if (!localStorage.getItem('active_user_key')) {
         localStorage.setItem('active_user_key', 'DEFAULT');
    }

    // 1. Obtener la clave de usuario de localStorage
    const activeUserKey = localStorage.getItem('active_user_key') || 'DEFAULT';
    
    // 2. Simulación de la función de llamada al endpoint /config
    const fetchConfig = () => {
        setIsLoading(true);
        setError(null);
        setUserName(activeUserKey);
        
        console.log(`Llamando al endpoint /config con user_key: ${activeUserKey}`);

        // --- INICIO: SIMULACIÓN DE LLAMADA FETCH ---
        // En una app real, esta sería la llamada fetch:
        /*
        fetch('/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ user_key: activeUserKey })
        })
        .then(res => res.json())
        .then(data => {
            setConfigData(data); 
            setIsLoading(false);
        })
        .catch(e => {
            console.error("Error al cargar la configuración:", e);
            setError("Error al cargar la configuración del servidor.");
            setIsLoading(false);
        });
        */
        
        // Usamos setTimeout para simular la latencia y el procesamiento de la respuesta
        setTimeout(() => {
            try {
                // Simulación de la respuesta JSON del endpoint /config
                const rawData = MOCK_API_RESPONSES[activeUserKey] || MOCK_API_RESPONSES["DEFAULT"];
                
                setConfigData(rawData); 
                setIsLoading(false);
            } catch (e) {
                console.error("Error simulado de /config:", e);
                setError("Error al procesar la configuración.");
                setIsLoading(false);
            }
        }, 1200); // 1.2 segundos de simulación de carga
        // --- FIN: SIMULACIÓN DE LLAMADA FETCH ---
    };

    fetchConfig();
  }, []); // Se ejecuta solo al montar el componente

  // Procesar la data del estado para obtener los datos de visualización
  const processedConfig = useMemo(() => {
    // Devolver un objeto base si los datos aún no están cargados
    if (!configData) {
        return { username: userName, groups: [] };
    }
    
    const userKeyData = configData; // configData es el objeto de grupos
    const groups = [];
    
    for (const groupName in userKeyData) {
        if (userKeyData.hasOwnProperty(groupName)) {
            const groupData = userKeyData[groupName];
            
            // Recopilar datos del Grupo, incluyendo su lista de paradas
            groups.push({
                name: groupName,
                id: groupName,
                coords: groupData.coords,
                stops: groupData.stops ? groupData.stops.sort() : [] // Manejo de stops nulas o indefinidas
            });
        }
    }

    return {
        username: userName,
        groups: groups, // Array de grupos, cada uno con sus paradas
    };
  }, [configData, userName]); // Recalcular cuando configData o userName cambien

  const toggleConfig = () => {
    setIsConfigVisible(!isConfigVisible);
    // Reiniciar la expansión del grupo al ocultar la configuración
    if (isConfigVisible) {
        setExpandedGroup(null); 
    }
  };
  
  const toggleGroupExpansion = (groupId) => {
    setExpandedGroup(prevId => (prevId === groupId ? null : groupId));
  };

  const handleSaveKey = () => {
    console.log('Guardando clave...');
    // Lógica simulada: Aquí se realizaría la llamada al endpoint para guardar
    alert('Clave guardada o actualizada (simulación).');
  };
  
  // Custom Alert function to comply with constraints
  const alert = (message) => {
    document.getElementById('message-box').innerHTML = `
      <div class="fixed inset-0 bg-gray-900 bg-opacity-70 z-50 flex items-center justify-center p-4">
        <div class="bg-white dark:bg-gray-800 rounded-xl p-6 shadow-2xl max-w-sm w-full text-center transform transition-all duration-300 scale-100">
          <p class="text-lg font-semibold dark:text-gray-100">${message}</p>
          <button onclick="document.getElementById('message-box').innerHTML = ''" class="mt-4 px-6 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 transition duration-150">
            Cerrar
          </button>
        </div>
      </div>
    `;
  };


  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 p-4 sm:p-8 flex items-center justify-center font-sans">
      <div id="message-box"></div> {/* Contenedor para mensajes/alertas */}

      <div className="w-full max-w-4xl bg-white dark:bg-gray-800 shadow-xl rounded-2xl p-6 sm:p-10 border border-gray-200 dark:border-gray-700">
        
        {/* Encabezado */}
        <div className="flex items-center mb-6 border-b pb-4 border-gray-100 dark:border-gray-700">
          <Settings className="w-8 h-8 text-indigo-500 mr-3" />
          <h1 className="text-2xl sm:text-3xl font-bold text-gray-800 dark:text-white">
            Panel de Gestión de Usuario
          </h1>
        </div>

        {/* Sección de Acciones y Configuración */}
        <div className="flex flex-col gap-6">
          
          {/* Fila de Botones: 'Guardar Clave' y 'Visualizar Configuración' */}
          <div className="flex flex-col sm:flex-row gap-4 sm:gap-6 items-center justify-between p-4 bg-gray-50 dark:bg-gray-700 rounded-xl shadow-md">
            
            <div className="w-full sm:w-auto">
              <p className="text-lg font-medium text-gray-700 dark:text-gray-200 mb-2 sm:mb-0">
                Clave Activa: <span className="font-extrabold text-indigo-600 dark:text-indigo-300">{userName}</span>
              </p>
            </div>

            <div className="flex flex-col sm:flex-row gap-3 w-full sm:w-auto">
              {/* Botón Guardar Clave */}
              <button
                onClick={handleSaveKey}
                className="flex items-center justify-center px-6 py-3 bg-red-600 text-white font-semibold rounded-lg shadow-md hover:bg-red-700 transition duration-200 w-full sm:w-auto transform hover:scale-[1.02]"
              >
                <Lock className="w-5 h-5 mr-2" />
                Guardar Clave
              </button>

              {/* Botón Visualizar Configuración */}
              <button
                onClick={toggleConfig}
                disabled={isLoading || error}
                className={`flex items-center justify-center px-6 py-3 font-semibold rounded-lg shadow-md transition duration-200 w-full sm:w-auto transform hover:scale-[1.02] 
                  ${(isLoading || error) ? 'bg-gray-400 dark:bg-gray-600 cursor-not-allowed' : 
                  isConfigVisible
                    ? 'bg-indigo-700 text-white hover:bg-indigo-800'
                    : 'bg-indigo-600 text-white hover:bg-indigo-700'
                }`}
              >
                {isConfigVisible ? (
                  <ChevronUp className="w-5 h-5 mr-2" />
                ) : (
                  <ChevronDown className="w-5 h-5 mr-2" />
                )}
                {isConfigVisible ? 'Ocultar Configuración' : 'Visualizar Configuración'}
              </button>
            </div>
          </div>
          
          {/* Contenedor de la Configuración (Se muestra/oculta) */}
          {isConfigVisible && (
            <div className="mt-4 border border-indigo-300 dark:border-indigo-500 rounded-xl p-4 sm:p-6 bg-indigo-50 dark:bg-gray-700/50 transition-all duration-300 ease-in-out">
              
              {/* Manejo de estados de carga/error */}
              {isLoading && (
                <div className="flex flex-col items-center justify-center h-40">
                    <Loader className="w-8 h-8 text-indigo-600 animate-spin" />
                    <p className="mt-3 text-lg font-medium text-indigo-600 dark:text-indigo-400">Cargando Configuración...</p>
                </div>
              )}

              {error && !isLoading && (
                <div className="flex flex-col items-center justify-center h-40 p-4 bg-red-100 dark:bg-red-900/50 rounded-lg">
                    <AlertTriangle className="w-8 h-8 text-red-600" />
                    <p className="mt-3 text-lg font-medium text-red-700 dark:text-red-400">Error: {error}</p>
                </div>
              )}
              
              {!isLoading && !error && (
                <>
                    <h2 className="text-xl sm:text-2xl font-semibold mb-4 text-indigo-700 dark:text-indigo-400 flex items-center">
                        <Users className="w-6 h-6 mr-2" />
                        Grupos Asignados y sus Paradas
                    </h2>

                    {/* Lista de Grupos como Acordeón */}
                    <div className="space-y-3">
                        {processedConfig.groups.map((group) => {
                          const isExpanded = expandedGroup === group.id;
                          return (
                            <div key={group.id} className="bg-white dark:bg-gray-800 rounded-lg shadow-md overflow-hidden transition-all duration-300 border border-gray-200 dark:border-gray-700">
                              
                              {/* Cabecera del Grupo (Clickeable) */}
                              <button
                                onClick={() => toggleGroupExpansion(group.id)}
                                className={`flex justify-between items-center w-full p-4 text-left transition duration-200 ${
                                    isExpanded ? 'bg-indigo-100 dark:bg-indigo-900/50' : 'hover:bg-gray-50 dark:hover:bg-gray-700'
                                }`}
                              >
                                <div className="flex items-center">
                                  <Users className={`w-5 h-5 mr-3 ${isExpanded ? 'text-indigo-600' : 'text-red-500 dark:text-red-400'}`} />
                                  <span className="font-bold text-lg text-gray-800 dark:text-gray-100">{group.name}</span>
                                </div>
                                <div className="flex items-center text-sm text-gray-500 dark:text-gray-400">
                                    <span className="mr-3 flex items-center"><MapPin className="w-4 h-4 mr-1"/> {group.stops.length} paradas</span>
                                    {isExpanded ? <ChevronUp className="w-5 h-5 text-indigo-600" /> : <ChevronDown className="w-5 h-5" />}
                                </div>
                              </button>

                              {/* Contenido de Paradas (Visible solo si está expandido) */}
                              {isExpanded && (
                                <div className="p-4 pt-0 border-t border-indigo-200 dark:border-indigo-700 bg-gray-50 dark:bg-gray-900/50">
                                  <h4 className="text-md font-semibold text-indigo-700 dark:text-indigo-300 mb-2 flex items-center">
                                    <Waypoints className="w-4 h-4 mr-2" />
                                    Paradas para este grupo:
                                  </h4>
                                  <div className="max-h-48 overflow-y-auto">
                                    <ul className="space-y-1">
                                      {group.stops.map((stopId) => (
                                        <li key={stopId} className="p-2 bg-white dark:bg-gray-800 rounded-md text-sm font-mono text-gray-800 dark:text-gray-200 border-l-4 border-green-500">
                                          {stopId}
                                        </li>
                                      ))}
                                    </ul>
                                  </div>
                                  <p className="text-xs text-gray-500 dark:text-gray-400 mt-2 flex items-center">
                                    <Map className="w-3 h-3 mr-1"/> Coordenadas principales del grupo: {group.coords}
                                  </p>
                                </div>
                              )}
                            </div>
                          );
                        })}
                    </div>
                </>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default App;
