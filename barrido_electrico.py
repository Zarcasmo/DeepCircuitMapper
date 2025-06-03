# barrido_electrico_core.py
import pandas as pd
import sys
import time

# ---- Dependencias de la Barra de Progreso y Colorama ----
try:
    from colorama import Fore, Style, init
    init(autoreset=True)  # Inicializa colorama para que los estilos se reseteen
except ImportError:
    # Fallback si colorama no está instalado para que el script no falle
    class Fore:  # type: ignore
        """Clase fallback para Fore si colorama no está disponible."""
        GREEN = ""; CYAN = ""; RESET = ""
    class Style:  # type: ignore
        """Clase fallback para Style si colorama no está disponible."""
        RESET_ALL = ""
    print("⚠️ Advertencia: La librería 'colorama' no está instalada. La barra de progreso no tendrá colores.")

# ----------------------------------------------------------
# Funciones del Barrido Eléctrico
# ----------------------------------------------------------

def print_progress_bar(iteration, total, prefix='', suffix='Completado', length=30, start_time=None, current_task_info=''):
    """
    Imprime una barra de progreso dinámica en la consola.

    Esta función muestra el progreso de una tarea iterativa, incluyendo el porcentaje completado,
    una representación visual de la barra, el tiempo transcurrido desde el inicio de la tarea,
    y opcionalmente, información sobre la subtarea o ítem actual. La barra se actualiza
    en la misma línea de la consola.

    Parámetros:
        iteration (int): El número actual de la iteración (1-based).
        total (int): El número total de iteraciones esperadas.
        prefix (str, optional): Texto a mostrar antes de la información de la tarea actual y la barra.
                                Por defecto ''.
        suffix (str, optional): Texto a mostrar después del porcentaje. Por defecto 'Completado'.
        length (int, optional): La longitud en caracteres de la barra de progreso visual. Por defecto 30.
        start_time (float, optional): El timestamp (obtenido con `time.time()`) del inicio de la tarea.
                                      Si se provee, se calcula y muestra el tiempo transcurrido. Por defecto None.
        current_task_info (str, optional): Información adicional sobre la tarea o ítem actual
                                           (ej. nombre del circuito) para mostrar en la línea de progreso.
                                           Por defecto ''.
    Retorna:
        None
    """
    if total == 0:  # Evitar división por cero si no hay iteraciones
        progress = 1.0
    else:
        progress = (iteration / total)
    
    filled_length = int(round(progress * length))
    # Asegura que la barra visual se construya correctamente
    arrow = '█' * filled_length
    spaces = '░' * (length - filled_length)
    percent = int(round(progress * 100))
    
    # Aplicar colores si colorama está disponible
    bar_color = Fore.GREEN
    percent_color = Fore.CYAN
    reset_color = Style.RESET_ALL

    bar_visual = f'[{bar_color}{arrow}{spaces}{reset_color}]'
    percent_text = f'{percent_color}{percent}%{reset_color}'
    
    time_str = ""
    if start_time:
        elapsed_time = time.time() - start_time
        elapsed_time_formatted = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        time_str = f' - Transcurrido: {elapsed_time_formatted}'
    
    task_info_str = f" {current_task_info} " if current_task_info else " "
    
    # Construye la línea de progreso completa
    # \r (carriage return) mueve el cursor al inicio de la línea actual para sobrescribirla.
    # Se añaden espacios al final para limpiar cualquier carácter residual de una línea anterior más larga.
    progress_line = f'\r{prefix}{task_info_str}{bar_visual} {percent_text} {suffix}{time_str}      '
    sys.stdout.write(progress_line)
    
    # Si se ha completado el total de iteraciones, imprime una nueva línea
    # para que la siguiente salida de la consola no sobrescriba la barra finalizada.
    if iteration == total and total > 0:
        sys.stdout.write('\n')
    
    sys.stdout.flush()  # Asegura que la salida se escriba inmediatamente en la consola.

def barrido_conectividad_por_circuito(
    circuito_co_inicial,
    df_elementos_corte_global,  # DataFrame global, no pre-filtrado por circuito aquí
    df_lineas_global,           # DataFrame global
    df_trafos_global,           # DataFrame global
    resultados_elementos_corte_global_lista,
    resultados_lineas_global_lista,
    resultados_transformadores_global_lista
    ):
    """
    Realiza un barrido de conectividad eléctrica para un circuito específico.

    Comienza desde un elemento de arranque (generalmente un interruptor principal de circuito)
    y explora la red eléctrica aguas abajo, identificando la conexión entre elementos de corte (EC)
    y líneas. También identifica transformadores conectados a las líneas energizadas.
    Utiliza un algoritmo de búsqueda en profundidad (DFS) implementado con una pila.

    Parámetros:
        circuito_co_inicial (str): El CODIGO_OPERATIVO del elemento de arranque del circuito.
        df_elementos_corte_global (pd.DataFrame): DataFrame con todos los elementos de corte de la red.
                                                  Debe contener 'CODIGO_OPERATIVO', 'G3E_FID', 'NODO1_ID',
                                                  'NODO2_ID', 'TIPO', 'EST_ESTABLE', 'CIRCUITO'.
        df_lineas_global (pd.DataFrame): DataFrame con todas las líneas de la red.
                                         Debe contener 'G3E_FID', 'NODO1_ID', 'NODO2_ID'.
        df_trafos_global (pd.DataFrame): DataFrame con todos los transformadores de la red.
                                         Debe contener 'G3E_FID', 'NODO1_ID', 'NODO2_ID'.
        resultados_elementos_corte_global_lista (list): Lista para acumular los diccionarios de los EC encontrados.
        resultados_lineas_global_lista (list): Lista para acumular los diccionarios de las líneas encontradas.
        resultados_transformadores_global_lista (list): Lista para acumular los diccionarios de los trafos encontrados.

    Retorna:
        None: La función modifica las listas de resultados directamente.
    """
    # Encuentra el elemento de arranque específico para este circuito_co_inicial
    # Se asume que el df_elementos_corte_global contiene todos los elementos.
    elementos_arranque = df_elementos_corte_global[df_elementos_corte_global['CODIGO_OPERATIVO'] == circuito_co_inicial]
    if elementos_arranque.empty:
        # No se imprime advertencia aquí para no interferir con la barra de progreso.
        # El manejo de circuitos no encontrados se puede hacer antes de llamar a esta función o al revisar resultados.
        return

    elemento_arranque = elementos_arranque.iloc[0].copy()
    fid_arranque = str(elemento_arranque['G3E_FID'])

    # Conjuntos para rastrear elementos visitados DENTRO de ESTE barrido de circuito específico
    visitados_ec_fids_este_circuito = set()
    visitados_lineas_fids_este_circuito = set()
    visitados_trafos_fids_este_circuito = set()

    # Prepara y añade el elemento de arranque a los resultados
    elemento_arranque_dict = elemento_arranque.to_dict()
    elemento_arranque_dict['Equipo_Padre'] = None  # El arranque no tiene padre en el barrido
    elemento_arranque_dict['Elementos_Aguas_Arriba'] = circuito_co_inicial # El camino aguas arriba es él mismo
    elemento_arranque_dict['Circuito_Origen_Barrido'] = circuito_co_inicial # Circuito que originó este barrido
    elemento_arranque_dict['Nodo_No_Explorado_Anillo'] = pd.NA  # Para análisis de anillos posterior
    resultados_elementos_corte_global_lista.append(elemento_arranque_dict)
    visitados_ec_fids_este_circuito.add(fid_arranque)

    # Inicializa la pila de exploración para el DFS
    pila_exploracion = []
    camino_co_aguas_arriba_para_hijos_de_arranque = [circuito_co_inicial]

    # Inicia la exploración desde NODO2 del interruptor de arranque (según regla de negocio)
    if pd.notna(elemento_arranque['NODO2_ID']) and str(elemento_arranque['NODO2_ID']).lower() != 'nan':
        pila_exploracion.append((
            str(elemento_arranque['G3E_FID']),      # FID del elemento actual (padre en la exploración)
            elemento_arranque['TIPO'],              # Tipo del elemento actual
            str(elemento_arranque['NODO2_ID']),     # Nodo desde el cual explorar
            circuito_co_inicial,                    # CO del EC padre directo en la jerarquía del barrido
            list(camino_co_aguas_arriba_para_hijos_de_arranque) # Camino de COs aguas arriba
        ))

    # Bucle principal del DFS
    while pila_exploracion:
        fid_padre_dfs, tipo_padre_dfs, nodo_a_explorar, co_ec_padre_jerarquico, camino_co_actual = pila_exploracion.pop()

        # 1. Explorar Líneas conectadas al nodo_a_explorar
        # Se busca en el DataFrame global de líneas.
        lineas_conectadas = df_lineas_global[
            (df_lineas_global['NODO1_ID'] == nodo_a_explorar) | (df_lineas_global['NODO2_ID'] == nodo_a_explorar)
        ]
        for _, linea_conectada_row_original in lineas_conectadas.iterrows():
            linea_conectada_row = linea_conectada_row_original.copy()
            linea_fid = str(linea_conectada_row['G3E_FID'])
            
            if linea_fid not in visitados_lineas_fids_este_circuito:
                visitados_lineas_fids_este_circuito.add(linea_fid)
                
                linea_dict = linea_conectada_row.to_dict()
                linea_dict['Equipo_Padre'] = co_ec_padre_jerarquico
                elementos_aguas_arriba_linea_str = ",".join(camino_co_actual)
                linea_dict['Elementos_Aguas_Arriba'] = elementos_aguas_arriba_linea_str
                linea_dict['Circuito_Origen_Barrido'] = circuito_co_inicial
                resultados_lineas_global_lista.append(linea_dict)
                
                # Determinar el otro nodo de la línea para continuar la exploración
                otro_nodo_de_linea_para_explorar = None
                if str(linea_conectada_row['NODO1_ID']) == nodo_a_explorar and \
                   pd.notna(linea_conectada_row['NODO2_ID']) and str(linea_conectada_row['NODO2_ID']).lower() != 'nan':
                    otro_nodo_de_linea_para_explorar = str(linea_conectada_row['NODO2_ID'])
                elif str(linea_conectada_row['NODO2_ID']) == nodo_a_explorar and \
                     pd.notna(linea_conectada_row['NODO1_ID']) and str(linea_conectada_row['NODO1_ID']).lower() != 'nan':
                    otro_nodo_de_linea_para_explorar = str(linea_conectada_row['NODO1_ID'])
                
                # Identificar transformadores conectados a esta línea
                nodos_de_conexion_en_linea = {nodo_a_explorar, otro_nodo_de_linea_para_explorar}
                nodos_de_conexion_en_linea.discard(None) # Remover None si 'otro_nodo' era None
                nodos_de_conexion_en_linea = {n for n in nodos_de_conexion_en_linea if str(n).lower() != 'nan'}


                for nodo_valido_en_linea in nodos_de_conexion_en_linea:
                    trafos_encontrados_en_nodo = df_trafos_global[
                        (df_trafos_global['NODO1_ID'] == nodo_valido_en_linea) | \
                        (df_trafos_global['NODO2_ID'] == nodo_valido_en_linea)
                    ]
                    for _, trafo_row_original in trafos_encontrados_en_nodo.iterrows():
                        trafo_row = trafo_row_original.copy()
                        trafo_fid = str(trafo_row['G3E_FID'])
                        if trafo_fid not in visitados_trafos_fids_este_circuito:
                            visitados_trafos_fids_este_circuito.add(trafo_fid)
                            
                            trafo_dict = trafo_row.to_dict()
                            trafo_dict['Linea_Conexion_FID'] = linea_fid
                            trafo_dict['Elementos_Aguas_Arriba'] = elementos_aguas_arriba_linea_str
                            trafo_dict['Circuito_Origen_Barrido'] = circuito_co_inicial
                            trafo_dict['Equipo_Padre_Linea'] = co_ec_padre_jerarquico
                            resultados_transformadores_global_lista.append(trafo_dict)
                                
                # Si la línea tiene un "otro extremo", añadirlo a la pila para continuar exploración
                if otro_nodo_de_linea_para_explorar: 
                    pila_exploracion.append((
                        linea_fid, 
                        'LINEA', # Tipo del elemento desde donde se origina esta exploración
                        otro_nodo_de_linea_para_explorar, 
                        co_ec_padre_jerarquico, # El padre jerárquico no cambia al pasar por una línea
                        list(camino_co_actual)
                    ))

        # 2. Explorar Elementos de Corte (EC) conectados al nodo_a_explorar
        # Se busca en el DataFrame global de ECs.
        ecs_conectados = df_elementos_corte_global[
            ((df_elementos_corte_global['NODO1_ID'] == nodo_a_explorar) | (df_elementos_corte_global['NODO2_ID'] == nodo_a_explorar)) &
            (df_elementos_corte_global['G3E_FID'] != fid_padre_dfs) # Evitar volver inmediatamente al EC padre por el mismo nodo
        ]
        for _, ec_conectado_row_original in ecs_conectados.iterrows():
            ec_conectado_row = ec_conectado_row_original.copy() # Trabajar con una copia para modificaciones temporales
            ec_fid = str(ec_conectado_row['G3E_FID'])
            
            if ec_fid not in visitados_ec_fids_este_circuito:
                visitados_ec_fids_este_circuito.add(ec_fid)
                
                ec_dict = ec_conectado_row.to_dict()
                ec_dict['Equipo_Padre'] = co_ec_padre_jerarquico
                ec_dict['Elementos_Aguas_Arriba'] = ",".join(camino_co_actual)
                ec_dict['Circuito_Origen_Barrido'] = circuito_co_inicial
                ec_dict['Nodo_No_Explorado_Anillo'] = pd.NA 

                # Lógica para manejo de ECs de interconexión (posibles puntos de anillo)
                # Si un EC pertenece a un circuito diferente al del barrido actual,
                # se trata como 'OPEN' para el análisis de anillos, conservando su estado original.
                estado_operativo_ec = ec_conectado_row['EST_ESTABLE'] # Estado original para la exploración
                if ('CIRCUITO' in ec_conectado_row and ec_conectado_row['CIRCUITO'] != circuito_co_inicial):
                    # Este EC es un potencial punto de transferencia o anillo con otro circuito
                    estado_operativo_ec = 'OPEN' # Tratar como OPEN para análisis de anillo
                    ec_dict['EST_ESTABLE_ORIGINAL'] = ec_conectado_row_original['EST_ESTABLE']
                    ec_dict['EST_ESTABLE'] = 'OPEN' # Para el DataFrame final y lógica de anillo
                
                # Si el EC está (o se considera) 'OPEN', identificar el nodo no explorado para el barrido de anillos
                if estado_operativo_ec == 'OPEN': 
                    nodo_no_explorado_para_anillo = pd.NA
                    if str(ec_conectado_row['NODO1_ID']) == nodo_a_explorar and \
                       pd.notna(ec_conectado_row['NODO2_ID']) and str(ec_conectado_row['NODO2_ID']).lower() != 'nan':
                        nodo_no_explorado_para_anillo = str(ec_conectado_row['NODO2_ID'])
                    elif str(ec_conectado_row['NODO2_ID']) == nodo_a_explorar and \
                         pd.notna(ec_conectado_row['NODO1_ID']) and str(ec_conectado_row['NODO1_ID']).lower() != 'nan':
                        nodo_no_explorado_para_anillo = str(ec_conectado_row['NODO1_ID'])
                    ec_dict['Nodo_No_Explorado_Anillo'] = nodo_no_explorado_para_anillo
                
                resultados_elementos_corte_global_lista.append(ec_dict)

                # Si el EC está efectivamente CERRADO (y no es de interconexión tratado como OPEN), continuar barrido
                if estado_operativo_ec == 'CLOSED': 
                    nuevo_co_ec_padre_jerarquico_para_hijos = str(ec_conectado_row['CODIGO_OPERATIVO'])
                    nuevo_camino_co_para_hijos = list(camino_co_actual) + [nuevo_co_ec_padre_jerarquico_para_hijos]
                    
                    otro_nodo_de_ec_para_explorar = pd.NA
                    if str(ec_conectado_row['NODO1_ID']) == nodo_a_explorar and \
                       pd.notna(ec_conectado_row['NODO2_ID']) and str(ec_conectado_row['NODO2_ID']).lower() != 'nan':
                        otro_nodo_de_ec_para_explorar = str(ec_conectado_row['NODO2_ID'])
                    elif str(ec_conectado_row['NODO2_ID']) == nodo_a_explorar and \
                         pd.notna(ec_conectado_row['NODO1_ID']) and str(ec_conectado_row['NODO1_ID']).lower() != 'nan':
                        otro_nodo_de_ec_para_explorar = str(ec_conectado_row['NODO1_ID'])
                    
                    if pd.notna(otro_nodo_de_ec_para_explorar):
                         pila_exploracion.append((
                             ec_fid, 
                             ec_conectado_row['TIPO'], 
                             otro_nodo_de_ec_para_explorar, 
                             nuevo_co_ec_padre_jerarquico_para_hijos, 
                             nuevo_camino_co_para_hijos
                        ))

def barrido_anillos_especifico(
    co_ec_open_original,
    nodo_inicio_anillo,
    df_elementos_corte_global, 
    df_lineas_global,         
    df_resultados_ecs_completos 
    ):
    """
    Realiza un barrido simplificado desde un nodo de un EC 'OPEN'.

    El objetivo es encontrar el primer EC con el que se conecta este nodo no explorado,
    para determinar la formación de un anillo o una transferencia.

    Parámetros:
        co_ec_open_original (str): CODIGO_OPERATIVO del EC 'OPEN' que origina este barrido de anillo.
        nodo_inicio_anillo (str): Nodo_ID desde el cual comenzar la exploración del anillo.
        df_elementos_corte_global (pd.DataFrame): DataFrame con todos los ECs.
        df_lineas_global (pd.DataFrame): DataFrame con todas las líneas.
        df_resultados_ecs_completos (pd.DataFrame): Resultados del primer barrido de conectividad,
                                                   usado para obtener información del EC que cierra el anillo.
    Retorna:
        tuple: (co_ec_encontrado, elementos_aguas_arriba_anillo, circuito_origen_anillo)
               Retorna (pd.NA, pd.NA, pd.NA) si no se encuentra un EC que cierre el anillo.
    """
    pila_exploracion_anillo = []
    # Conjunto para evitar ciclos dentro de ESTE barrido de anillo específico
    visitados_fids_este_anillo_dfs = set() 
    # No queremos "encontrar" el mismo EC 'OPEN' como el otro extremo del anillo inmediatamente.
    # El G3E_FID del co_ec_open_original se podría usar si se tiene, pero el CO es más directo aquí.
    # visitados_fids_este_anillo_dfs.add(co_ec_open_original) # Esto sería si co_ec_open_original fuera FID

    # La tupla en la pila: (FID_del_elemento_anterior, TIPO_elemento_anterior, nodo_a_explorar_ahora)
    pila_exploracion_anillo.append( (None, "NODO_INICIAL_ANILLO", nodo_inicio_anillo) )
    
    max_iteraciones_anillo = 100 # Límite para prevenir bucles infinitos en topologías complejas
    iter_count = 0

    while pila_exploracion_anillo and iter_count < max_iteraciones_anillo:
        iter_count += 1
        _, _, nodo_actual_anillo = pila_exploracion_anillo.pop()

        # Marcar el nodo como visitado en este contexto de barrido de anillo para evitar reprocesarlo
        # (Esto es una simplificación, normalmente se marcan FIDs de elementos, no nodos)
        # Es mejor marcar los FIDs de las líneas/ECs atravesados.

        # 1. Buscar ECs Conectados directamente al nodo_actual_anillo
        #    Estos son los candidatos a cerrar el anillo.
        ecs_conectados_directo_anillo = df_elementos_corte_global[
            ((df_elementos_corte_global['NODO1_ID'] == nodo_actual_anillo) | \
             (df_elementos_corte_global['NODO2_ID'] == nodo_actual_anillo)) & \
            (df_elementos_corte_global['CODIGO_OPERATIVO'] != co_ec_open_original) 
        ]
        for _, ec_row in ecs_conectados_directo_anillo.iterrows():
            co_ec_encontrado_anillo = str(ec_row['CODIGO_OPERATIVO'])
            
            # Se encontró un EC. Obtener su información del barrido principal.
            info_ec_encontrado_barrido1 = df_resultados_ecs_completos[
                df_resultados_ecs_completos['CODIGO_OPERATIVO'] == co_ec_encontrado_anillo
            ]
            if not info_ec_encontrado_barrido1.empty:
                data_ec_encontrado = info_ec_encontrado_barrido1.iloc[0]
                elementos_aguas_arriba_anillo = data_ec_encontrado.get('Elementos_Aguas_Arriba', pd.NA)
                circuito_origen_anillo = data_ec_encontrado.get('Circuito_Origen_Barrido', pd.NA)
                return co_ec_encontrado_anillo, elementos_aguas_arriba_anillo, circuito_origen_anillo
            else: 
                # EC encontrado en la red física pero no en los resultados del barrido1 (poco común si los datos son consistentes)
                return co_ec_encontrado_anillo, pd.NA, pd.NA 

        # 2. Si no hay EC directo, buscar Líneas Conectadas al nodo_actual_anillo para seguir explorando
        lineas_conectadas_anillo = df_lineas_global[
            ((df_lineas_global['NODO1_ID'] == nodo_actual_anillo) | \
             (df_lineas_global['NODO2_ID'] == nodo_actual_anillo)) 
        ]
        for _, linea_row_anillo in lineas_conectadas_anillo.iterrows():
            linea_fid_anillo = str(linea_row_anillo['G3E_FID'])
            if linea_fid_anillo not in visitados_fids_este_anillo_dfs: # Evitar revisitar la misma línea en este barrido
                visitados_fids_este_anillo_dfs.add(linea_fid_anillo)
                
                otro_nodo_linea_anillo = None
                if str(linea_row_anillo['NODO1_ID']) == nodo_actual_anillo and \
                   pd.notna(linea_row_anillo['NODO2_ID']) and str(linea_row_anillo['NODO2_ID']).lower() != 'nan':
                    otro_nodo_linea_anillo = str(linea_row_anillo['NODO2_ID'])
                elif str(linea_row_anillo['NODO2_ID']) == nodo_actual_anillo and \
                     pd.notna(linea_row_anillo['NODO1_ID']) and str(linea_row_anillo['NODO1_ID']).lower() != 'nan':
                    otro_nodo_linea_anillo = str(linea_row_anillo['NODO1_ID'])
                
                if otro_nodo_linea_anillo:
                    pila_exploracion_anillo.append((linea_fid_anillo, 'LINEA_ANILLO', otro_nodo_linea_anillo))
        
        # 3. Si no hay línea, buscar ECs CERRADOS para continuar la exploración a través de ellos.
        ecs_para_atravesar_anillo = df_elementos_corte_global[
            ((df_elementos_corte_global['NODO1_ID'] == nodo_actual_anillo) | \
             (df_elementos_corte_global['NODO2_ID'] == nodo_actual_anillo)) & \
            (df_elementos_corte_global['CODIGO_OPERATIVO'] != co_ec_open_original) & \
            (df_elementos_corte_global['EST_ESTABLE'] == 'CLOSED') # Solo se puede atravesar ECs cerrados
        ]
        for _, ec_row_atravesar in ecs_para_atravesar_anillo.iterrows():
            ec_fid_atravesar = str(ec_row_atravesar['G3E_FID'])
            if ec_fid_atravesar not in visitados_fids_este_anillo_dfs: # Evitar revisitar
                visitados_fids_este_anillo_dfs.add(ec_fid_atravesar)
                
                otro_nodo_ec_atravesar = None
                if str(ec_row_atravesar['NODO1_ID']) == nodo_actual_anillo and \
                   pd.notna(ec_row_atravesar['NODO2_ID']) and str(ec_row_atravesar['NODO2_ID']).lower() != 'nan':
                    otro_nodo_ec_atravesar = str(ec_row_atravesar['NODO2_ID'])
                elif str(ec_row_atravesar['NODO2_ID']) == nodo_actual_anillo and \
                     pd.notna(ec_row_atravesar['NODO1_ID']) and str(ec_row_atravesar['NODO1_ID']).lower() != 'nan':
                    otro_nodo_ec_atravesar = str(ec_row_atravesar['NODO1_ID'])
                
                if otro_nodo_ec_atravesar:
                    pila_exploracion_anillo.append((ec_fid_atravesar, ec_row_atravesar['TIPO'], otro_nodo_ec_atravesar))
    
    # if iter_count >= max_iteraciones_anillo:
    #     print(f"⚠️ Advertencia: Barrido de anillo para {co_ec_open_original} desde nodo {nodo_inicio_anillo} alcanzó el límite de iteraciones.")
    return pd.NA, pd.NA, pd.NA


def generar_dfs_resultados_finales(df_circuitos, df_elementos_corte_global, df_lineas_global, df_trafos_global, verbose=False):
    """
    Orquesta el proceso completo de barrido de conectividad y análisis de anillos.

    Primero, realiza un barrido de conectividad para cada circuito especificado.
    Luego, analiza los Elementos de Corte (EC) que quedaron en estado 'OPEN' para
    identificar posibles formaciones de anillos o transferencias con otros ECs.

    Parámetros:
        df_circuitos (pd.DataFrame): DataFrame con la lista de circuitos a procesar (columna 'Circuito').
        df_elementos_corte_global (pd.DataFrame): DataFrame con todos los ECs de la red.
        df_lineas_global (pd.DataFrame): DataFrame con todas las líneas de la red.
        df_trafos_global (pd.DataFrame): DataFrame con todos los transformadores de la red.
        verbose (bool, optional): Si es True, imprime mensajes detallados durante el análisis de anillos.
                                  Por defecto False.

    Retorna:
        tuple: Una tupla conteniendo tres DataFrames:
               - df_final_elementos_corte (pd.DataFrame): Resultados del barrido para ECs,
                 incluyendo información de anillos.
               - df_final_lineas (pd.DataFrame): Resultados del barrido para líneas.
               - df_final_trafos (pd.DataFrame): Resultados del barrido para transformadores.
               En caso de error en la carga de datos inicial, devuelve DataFrames vacíos.
    """
    if df_circuitos is None or df_elementos_corte_global is None or \
       df_lineas_global is None or df_trafos_global is None:
        print("❌ Error en la carga de datos inicial. No se puede continuar el barrido.")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        
    # Listas para acumular resultados de todos los barridos de circuito
    resultados_elementos_corte_acumulados_lista = []
    resultados_lineas_acumulados_lista = []
    resultados_transformadores_acumulados_lista = []

    # --- FASE 1: BARRIDO DE CONECTIVIDAD PRINCIPAL ---
    print("\n🔄 Iniciando primer barrido de conectividad...")
    total_circuitos_barrido1 = len(df_circuitos)
    start_time_barrido1 = time.time()
    if total_circuitos_barrido1 > 0:
        print_progress_bar(0, total_circuitos_barrido1, prefix='Barrido Principal:', 
                           start_time=start_time_barrido1, current_task_info="Iniciando...")
    
    for i, (_, row_circuito) in enumerate(df_circuitos.iterrows()):
        circuito_co_inicial_actual = str(row_circuito['Circuito'])
        task_info_barrido1 = f"Cto: {circuito_co_inicial_actual}"
        
        # Llama a la función de barrido para el circuito actual.
        # Se pasan los DataFrames globales; la función barrido_conectividad_por_circuito
        # usa 'circuito_co_inicial_actual' para encontrar el punto de arranque correcto.
        barrido_conectividad_por_circuito(
            circuito_co_inicial_actual,
            df_elementos_corte_global, 
            df_lineas_global,
            df_trafos_global,
            resultados_elementos_corte_acumulados_lista,
            resultados_lineas_acumulados_lista,
            resultados_transformadores_acumulados_lista
        )
        if total_circuitos_barrido1 > 0:
            print_progress_bar(i + 1, total_circuitos_barrido1, prefix='Barrido Principal:', 
                               start_time=start_time_barrido1, current_task_info='')
    
    # Convertir listas de resultados a DataFrames
    df_final_elementos_corte = pd.DataFrame(resultados_elementos_corte_acumulados_lista)
    df_final_lineas = pd.DataFrame(resultados_lineas_acumulados_lista)
    df_final_trafos = pd.DataFrame(resultados_transformadores_acumulados_lista)

    # Inicializar columnas para información de anillos en el DataFrame de ECs
    if not df_final_elementos_corte.empty:
        df_final_elementos_corte['Equipo_anillo'] = pd.NA
        df_final_elementos_corte['Elementos_Aguas_Arriba_anillo'] = pd.NA
        df_final_elementos_corte['Circuito_anillo'] = pd.NA
    else:
        # Si no hay ECs, el análisis de anillos no es aplicable.
        # Los DFs de líneas y trafos pueden tener datos, así que se retornan.
        print("ℹ️ No se encontraron elementos de corte en el primer barrido. Análisis de anillos omitido.")
        return df_final_elementos_corte, df_final_lineas, df_final_trafos

    # --- FASE 2: ANÁLISIS DE ANILLOS PARA ECs 'OPEN' ---
    print("\n🔄 Iniciando análisis de anillos y transferencias...")
    total_circuitos_barrido2 = len(df_circuitos) 
    start_time_barrido2 = time.time()
    if total_circuitos_barrido2 > 0:
        print_progress_bar(0, total_circuitos_barrido2, prefix='Análisis Anillos:', 
                           start_time=start_time_barrido2, current_task_info="Iniciando...")

    # Itera nuevamente sobre los circuitos para procesar los ECs 'OPEN' que pertenecen a cada uno.
    # Esto es para contextualizar el barrido de anillos, aunque la barra muestra el progreso general de circuitos.
    for i, (_, row_circuito) in enumerate(df_circuitos.iterrows()):
        circuito_para_analisis_anillo = str(row_circuito['Circuito'])
        task_info_anillos_fase = f"Cto: {circuito_para_analisis_anillo}"

        # Filtra ECs 'OPEN' con 'Nodo_No_Explorado_Anillo' válido Y que pertenecen
        # al 'CIRCUITO' original que se está iterando para el análisis de anillos.
        # Se asume que la columna 'CIRCUITO' en df_final_elementos_corte es el circuito de pertenencia original del EC.
        if 'CIRCUITO' in df_final_elementos_corte.columns:
            ecs_open_del_circuito_actual = df_final_elementos_corte[
                (df_final_elementos_corte['EST_ESTABLE'] == 'OPEN') &
                (df_final_elementos_corte['Nodo_No_Explorado_Anillo'].notna()) &
                (df_final_elementos_corte['CIRCUITO'] == circuito_para_analisis_anillo)
            ].copy() # .copy() para evitar SettingWithCopyWarning en actualizaciones posteriores
        else:
            # Si la columna 'CIRCUITO' falta, no se puede filtrar por circuito para los anillos.
            # Se podría optar por procesar todos los ECs open o mostrar una advertencia más fuerte.
            if verbose: print(f"⚠️ Advertencia: Columna 'CIRCUITO' no encontrada en df_final_elementos_corte. "
                              f"Análisis de anillos para {circuito_para_analisis_anillo} podría ser incompleto o global.")
            ecs_open_del_circuito_actual = df_final_elementos_corte[ # Procesar todos los open si 'CIRCUITO' falta
                (df_final_elementos_corte['EST_ESTABLE'] == 'OPEN') &
                (df_final_elementos_corte['Nodo_No_Explorado_Anillo'].notna())
            ].copy()
            if ecs_open_del_circuito_actual.empty and verbose : #Solo si no hay ecs_open para procesar.
                 print(f"    No hay ECs 'OPEN' con nodo no explorado para el circuito {circuito_para_analisis_anillo}.")


        # Itera sobre los ECs 'OPEN' identificados para este circuito y realiza el barrido de anillo
        for index_ec_open, row_ec_open_actual in ecs_open_del_circuito_actual.iterrows():
            co_ec_open_actual = row_ec_open_actual['CODIGO_OPERATIVO']
            nodo_a_explorar_anillo = row_ec_open_actual['Nodo_No_Explorado_Anillo']
            
            # Mensajes detallados si verbose está activado
            if verbose: 
                # Usar \r para intentar sobrescribir mensajes de verbose, aunque puede no ser perfecto.
                sys.stdout.write(f"\r    Analizando anillo para EC 'OPEN': {co_ec_open_actual} (Cto: {circuito_para_analisis_anillo}) desde nodo {nodo_a_explorar_anillo}...\n")
                sys.stdout.flush()

            equipo_anillo_encontrado, eaa_anillo_str, circuito_del_anillo = barrido_anillos_especifico(
                co_ec_open_actual,
                nodo_a_explorar_anillo,
                df_elementos_corte_global, # DataFrame global de ECs
                df_lineas_global,          # DataFrame global de Líneas
                df_final_elementos_corte   # Resultados del barrido principal para consulta
            )
            
            if pd.notna(equipo_anillo_encontrado):
                if verbose: 
                    sys.stdout.write(f"\r      Anillo encontrado para {co_ec_open_actual}: Equipo={equipo_anillo_encontrado}, Cto.Anillo={circuito_del_anillo}\n")
                    sys.stdout.flush()
                # Actualizar el DataFrame principal con la información del anillo
                df_final_elementos_corte.loc[index_ec_open, 'Equipo_anillo'] = equipo_anillo_encontrado
                df_final_elementos_corte.loc[index_ec_open, 'Elementos_Aguas_Arriba_anillo'] = eaa_anillo_str
                df_final_elementos_corte.loc[index_ec_open, 'Circuito_anillo'] = circuito_del_anillo
        
        # Actualizar la barra de progreso después de procesar todos los ECs 'OPEN' de este circuito
        if total_circuitos_barrido2 > 0:
            print_progress_bar(i + 1, total_circuitos_barrido2, prefix='Análisis Anillos:', 
                               start_time=start_time_barrido2, current_task_info='')
            
    # Eliminar duplicados después de todos los procesamientos.
    # Es importante que las columnas usadas para identificar duplicados existan.
    if not df_final_elementos_corte.empty:
        cols_subset_ec = ['G3E_FID', 'Circuito_Origen_Barrido', 'Equipo_Padre', 'Elementos_Aguas_Arriba']
        cols_subset_ec_existentes = [col for col in cols_subset_ec if col in df_final_elementos_corte.columns]
        if cols_subset_ec_existentes: # Solo si hay columnas válidas para el subset
             df_final_elementos_corte = df_final_elementos_corte.drop_duplicates(subset=cols_subset_ec_existentes, keep='first')
    
    if not df_final_lineas.empty:
        cols_subset_li = ['G3E_FID', 'Circuito_Origen_Barrido', 'Equipo_Padre', 'Elementos_Aguas_Arriba']
        cols_subset_li_existentes = [col for col in cols_subset_li if col in df_final_lineas.columns]
        if cols_subset_li_existentes:
            df_final_lineas = df_final_lineas.drop_duplicates(subset=cols_subset_li_existentes, keep='first')
        
    return df_final_elementos_corte, df_final_lineas, df_final_trafos